//! ravel-cli: inspect segments, decode commit records, list catalog entries.

use std::time::Duration;

use clap::{Parser, Subcommand};
use ravel_cli::maintain::SignalArg;
use ravel_cli::{
    catalog, hold, idem, maintain, now_ns, parse_max_flush_lifetime_ns, rlog_footprint, store,
    tenancy, tenant_token,
};
use ravel_logseg::block::NumStat;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, COMP_NONE, COMP_ZSTD, SortBucketWidth, SortKeyType, kind};
use ravel_logseg::record::FieldType;
use ravel_logseg::rlog_bloom::RlogBloomSection;
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{RlogConfig, read_section};
use ravel_proto::segment::v1::Footer;
use ravel_types::{Signal, TenantId, TimeRange};

// jemalloc is compiled into this binary so the allocator is part of the
// binary's identity rather than an environment accident (#972); library crates
// must never do this (their test binaries install their own global allocator).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Decode caps for the whole-read RLOG sections, matching `RlogReader`'s own
// limits (crates/ravel-logseg/src/reader.rs). The inspector reads STREAM_DIR,
// FIELD_DIR, and SKIP_IDX; a decode past these caps is `Corrupted`.
const RLOG_MAX_STREAMS: u64 = 1 << 24;
const RLOG_MAX_FIELDS: u64 = 1 << 20;
const RLOG_MAX_BLOCKS: u64 = 1 << 24;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

// Frozen wire values from proto/ravel/segment.proto's `SectionKind` enum.
const SECTION_KIND_LABEL_DICT: u32 = 1;
// Retired with RSEG v1 (ADR-0027); kept only to name a stray kind-2 section.
const SECTION_KIND_SERIES_TABLE: u32 = 2;
const SECTION_KIND_TS_PAGES: u32 = 3;
const SECTION_KIND_VAL_PAGES: u32 = 4;
const SECTION_KIND_SERIES_IDS: u32 = 5;
const SECTION_KIND_SERIES_META: u32 = 6;
const SECTION_KIND_HIST_PAGES: u32 = 7;
// v5 sparse-catalog kinds (docs/segment-format.md).
const SECTION_KIND_SERIES_IDX: u32 = 8;
const SECTION_KIND_SERIES_META_CHUNKS: u32 = 9;
// v6 addition (ADR-0047): optional per-object exemplar records.
const SECTION_KIND_EXEMPLARS: u32 = 10;

#[derive(Debug, Parser)]
#[command(name = "ravel-cli", about = "Ravel dev inspection CLI")]
struct Cli {
    #[command(flatten)]
    store: store::StoreArgs,

    #[command(flatten)]
    tenancy: TenancyArgs,

    /// Path to the external credential profile file (ADR-2040 decision D1),
    /// the JSON list of named profiles `tenant parquet-grant add` resolves
    /// `--profile` against. ravel-server reads the same file, from its own
    /// `--parquet-profiles` flag, to read Parquet tables' files. A top-level
    /// flag, given before the subcommand, like the tenant-hash flags.
    #[arg(long, value_name = "PATH", env = "RAVEL_PARQUET_PROFILES")]
    parquet_profiles: Option<std::path::PathBuf>,

    #[command(subcommand)]
    command: Command,
}

/// Global tenant-hash scheme selection (ADR-0050 section 3). Every
/// subcommand that computes a `t/<tenant_hash>/` prefix resolves the bucket's
/// scheme from `sys/tenancy` before running (see `resolve_and_install_scheme`);
/// these flags supply the deployment key, or the unkeyed opt-out, that
/// resolution needs, mirroring the server's own startup flags. A keyed bucket
/// run with neither flag refuses rather than hashing under the wrong (v1)
/// derivation. `tenancy show` needs neither and takes its own key flag: it
/// reads the marker directly to discover the scheme in the first place.
///
/// These are top-level flags, given before the subcommand
/// (`ravel-cli --tenant-hash-key-file k hold set ...`); they are intentionally
/// not `global` so they cannot collide with `tenancy show`'s own key flag.
#[derive(Debug, Parser)]
struct TenancyArgs {
    /// Path to the bucket's 32-byte deployment key (64 hex characters or 32 raw
    /// bytes), needed to address a v2-keyed bucket's tenant prefixes.
    #[arg(long, value_name = "PATH")]
    tenant_hash_key_file: Option<std::path::PathBuf>,

    /// Assert the bucket is v1-unkeyed. An unkeyed or absent marker resolves to
    /// v1 without this, but it makes the expectation explicit; mutually
    /// exclusive with --tenant-hash-key-file.
    #[arg(long)]
    tenant_hash_unkeyed: bool,
}

/// Whether a subcommand computes a `t/<tenant_hash>/` prefix and therefore
/// needs the bucket's scheme resolved and installed first. The
/// inspection commands that take an explicit object key or a local file, and
/// `tenancy show` (which reads the marker directly), do not.
fn command_hashes_tenant(command: &Command) -> bool {
    match command {
        Command::Catalog { .. }
        | Command::Maintain { .. }
        | Command::Hold { .. }
        | Command::Erase { .. }
        | Command::Provision { .. }
        // The tenant config record lives at `t/<tenant_hash>/config`, so
        // typed-attr-column hashes a tenant (unlike gc-config, whose object is
        // at the bucket root).
        | Command::TypedAttrColumn { .. }
        | Command::ClusteringKey { .. }
        | Command::BloomScope { .. }
        | Command::Load { .. }
        // `export` resolves the catalog under `t/<tenant_hash>/logs/...` from
        // its `--tenant`, exactly as `catalog list` does.
        | Command::Export { .. }
        // Every Parquet-table object is under `t/<tenant_hash>/pq/`
        // (ADR-2040 decision D1), so every `parquet` subcommand hashes a
        // tenant.
        | Command::Parquet { .. } => true,
        // `commit reconstruct` computes a `t/<tenant_hash>/` prefix from its
        // `--tenant`, so it needs the bucket's scheme resolved first; the
        // other `commit` variants take an explicit key/path and do not.
        Command::Commit { command } => matches!(command, CommitCommand::Reconstruct { .. }),
        // `rlog footprint --tenant` resolves the tenant's catalog; given
        // object keys or paths instead, it hashes nothing.
        Command::Rlog { command } => matches!(
            command,
            RlogCommand::Footprint {
                tenant: Some(_),
                ..
            }
        ),
        Command::Segment { .. }
        | Command::Rspan { .. }
        | Command::Store { .. }
        | Command::Idem { .. }
        // `inspect cstat` takes an explicit object key, exactly like
        // `segment inspect`/`rlog inspect`: no tenant to hash.
        | Command::Inspect { .. }
        // `tenancy show` decodes the marker itself and takes no `--tenant`;
        // `tenancy resolve` (issue #1180) resolves the scheme itself inline
        // (mirroring `show`), rather than going through this shared gate, so
        // its dispatch arm stays self-contained and directly testable.
        | Command::Tenancy { .. }
        // sys/gc is a bucket-root object, not under any tenant prefix, so
        // gc-config never hashes a tenant. sys/auth (ADR-0072 decision 4) is
        // the same shape: deployment-wide, at the bucket root, never under a
        // tenant prefix, so `tenant token` never hashes a tenant either.
        | Command::GcConfig { .. }
        // `cache reclaim-legacy` operates on a local directory, not object
        // storage: no tenant prefix, no store built.
        | Command::Cache { .. } => false,
        // `tenant token` reads and writes bucket-root `sys/auth`;
        // `tenant parquet-grant` reads and writes the grants record at
        // `t/<tenant_hash>/pq/grants`, so only the second half hashes.
        Command::Tenant { command } => match command {
            TenantCommand::Token { .. } => false,
            TenantCommand::ParquetGrant { .. } => true,
        },
    }
}

/// Whether `command` writes to the object store (issue #1184). Used ahead of
/// dispatch to require the bucket's `sys/tenancy` marker (or an explicit
/// `--tenant-hash-key-file` / `--tenant-hash-unkeyed`) before the first write,
/// rather than the lenient absent-marker default [`tenancy::resolve_scheme`]
/// gives read-only commands: a fresh bucket defaults to keyed on the server,
/// so a write there under the silent v1-unkeyed fallback lands under a prefix
/// nothing addresses once the bucket is eventually bootstrapped keyed.
///
/// Exhaustive match, no catch-all: adding a `Command` variant must fail to
/// compile here until someone classifies it, so the gate fails closed on
/// future commands instead of silently treating them as reads (this is the
/// hole issue #1184's original `_ => false` left open). Mirrors
/// `command_hashes_tenant` above: every group names why it is a write or not.
fn command_is_write(command: &Command) -> bool {
    match command {
        Command::Load { .. } => true,
        // `export` reads a resolved snapshot's objects and writes only to the
        // local `--parquet` path; it publishes nothing to object storage.
        Command::Export { .. } => false,
        // Both `provision` shapes write a durable record (or, for `adopt`, a
        // control record and audit entry via `reshard`); neither has a
        // read-only variant.
        Command::Provision { .. } => true,
        Command::TypedAttrColumn { command } => {
            matches!(command, TypedAttrColumnCommand::Set { .. })
        }
        // `set` and `clear` swap the tenant's config record; `show` reads it.
        Command::ClusteringKey { command } => matches!(
            command,
            ClusteringKeyCommand::Set { .. } | ClusteringKeyCommand::Clear { .. }
        ),
        Command::BloomScope { command } => matches!(command, BloomScopeCommand::Set { .. }),
        Command::Hold { command } => {
            matches!(command, HoldCommand::Set { .. } | HoldCommand::Clear { .. })
        }
        Command::Erase { command } => matches!(command, EraseCommand::Submit { .. }),
        // sys/gc is a bucket-root object (see `command_hashes_tenant`), but
        // `gc-config set` still clears this same write gate: see
        // `gc_config_set_refuses_on_marker_less_bucket` and the final report
        // for the tension this raises with deliverable 3 of issue #1184's
        // follow-up (closing the `command_is_write` catch-all).
        Command::GcConfig { command } => matches!(command, GcConfigCommand::Set { .. }),
        Command::Commit { command } => match command {
            // Decode-only shapes never write, only read an existing record.
            CommitCommand::Decode { .. }
            | CommitCommand::DecodeCompaction { .. }
            | CommitCommand::DecodeTombstone { .. } => false,
            // Writes CreateIfAbsent-only commit records under
            // `t/<tenant_hash>/<signal>/<shard>/...`, computed from
            // `--tenant` (see `command_hashes_tenant`).
            CommitCommand::Reconstruct { .. } => true,
        },
        Command::Catalog { command } => match command {
            // `list`/`inspect`/`verify` only read the catalog and commit
            // records; they never publish a snapshot.
            CatalogCommand::List { .. }
            | CatalogCommand::Inspect { .. }
            | CatalogCommand::Verify { .. } => false,
            // Publishes a new snapshot HEAD and part objects under
            // `t/<tenant_hash>/<signal>/...`.
            CatalogCommand::Fold { .. } => true,
        },
        Command::Maintain { command } => match command {
            // Writes L1 segment objects and a compaction record under
            // `t/<tenant_hash>/<signal>/<shard>/...`, plus the bucket's claim
            // under `sys/maintain/claims/` (unless `--no-claim`), EXCEPT
            // under `--dry-run`, which takes no claim and
            // only reports the plan it would run. Gating a dry run would
            // stop an operator inspecting that plan on a marker-less bucket,
            // which is the one case where they most need to look before acting.
            MaintainCommand::CompactBucket { dry_run, .. }
            | MaintainCommand::CompactTenant { dry_run, .. } => !dry_run,
            // Deletes orphaned/superseded/unreferenced segment objects under
            // `t/<tenant_hash>/<signal>/<shard>/...`; `--dry-run` only lists
            // what it would delete.
            MaintainCommand::Sweep { dry_run, .. } => !dry_run,
            // Rewrites objects to the target format version and raises the
            // recorded format floor under `t/<tenant_hash>/...`; `--dry-run`
            // only runs the read-only re-audit.
            MaintainCommand::Migrate { dry_run, .. } => !dry_run,
            // Read-only inspection/reporting: `status` reports maintenance
            // state, `audit-versions` audits live format versions, and
            // `verify-custody` re-verifies the content-addressed chain; none
            // writes or deletes.
            MaintainCommand::Status { .. }
            | MaintainCommand::AuditVersions { .. }
            | MaintainCommand::VerifyCustody { .. } => false,
        },
        // `store qualify` writes `sys/qualification`, a bucket-root object,
        // never under `t/<tenant_hash>/`; gating it would also break the
        // documented bootstrap order (it runs before the first server start,
        // which is what writes `sys/tenancy` in the first place).
        Command::Store { .. } => false,
        Command::Tenant { command } => match command {
            TenantCommand::Token { command } => match command {
                // `list` only reads `sys/auth`.
                TenantTokenCommand::List { .. } => false,
                // `upsert`/`revoke` write `sys/auth`, a bucket-root object,
                // never under `t/<tenant_hash>/`.
                TenantTokenCommand::Upsert { .. } | TenantTokenCommand::Revoke { .. } => false,
            },
            // `add`/`remove` replace the grants record at
            // `t/<tenant_hash>/pq/grants` under CAS, and `add` also writes a
            // probe object to the bucket root; `ls` only reads the record.
            TenantCommand::ParquetGrant { command } => match command {
                TenantParquetGrantCommand::Add { .. }
                | TenantParquetGrantCommand::Remove { .. } => true,
                TenantParquetGrantCommand::Ls { .. } => false,
            },
        },
        Command::Parquet { command } => match command {
            // Reads manifest versions only.
            ParquetCommand::Ls { .. } => false,
            // Deletes superseded manifest versions under
            // `t/<tenant_hash>/pq/t/`.
            ParquetCommand::Sweep { .. } => true,
            // Deletes flagged manifest version keys, or with `--stray` the
            // keys under no valid table name, with `--delete`, or one named
            // version with `--delete-version`; otherwise only lists.
            ParquetCommand::Repair {
                delete,
                delete_version,
                ..
            } => *delete || delete_version.is_some(),
        },
        // Pure inspection commands that take an explicit key/path or decode a
        // marker directly (`tenancy show`/`resolve` resolve the scheme
        // inline rather than through this gate, per `command_hashes_tenant`),
        // plus `cache reclaim-legacy`, which never touches object storage.
        Command::Segment { .. }
        | Command::Rlog { .. }
        | Command::Rspan { .. }
        | Command::Idem { .. }
        | Command::Tenancy { .. }
        | Command::Cache { .. }
        // `inspect cstat` only reads the named object.
        | Command::Inspect { .. } => false,
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect an RSEG segment (trailer, footer, sections, series count).
    Segment {
        #[command(subcommand)]
        command: SegmentCommand,
    },
    /// Inspect an RLOG log segment (footer, sections, skip index, directories).
    Rlog {
        #[command(subcommand)]
        command: RlogCommand,
    },
    /// Inspect an RSPAN span segment (footer, sections, skip index).
    Rspan {
        #[command(subcommand)]
        command: RspanCommand,
    },
    /// Fetch and decode a commit record.
    Commit {
        #[command(subcommand)]
        command: CommitCommand,
    },
    /// List commit records via the catalog.
    Catalog {
        #[command(subcommand)]
        command: CatalogCommand,
    },
    /// Inspect a standalone object by key or local path, outside the
    /// segment/rlog/rspan/commit/catalog families above.
    Inspect {
        #[command(subcommand)]
        command: InspectCommand,
    },
    /// Run and inspect maintenance: compaction, sweep, retention, version audit.
    Maintain {
        #[command(subcommand)]
        command: MaintainCommand,
    },
    /// Object store backend qualification (ADR-0050 section 6).
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
    /// Place, clear, and list legal holds (ADR-0048 decision 2):
    /// the only production mechanism to set a hold.
    Hold {
        #[command(subcommand)]
        command: HoldCommand,
    },
    /// Submit and inspect selective (GDPR/CCPA subject) erasure requests
    /// (ADR-0064 decision 1). Runs under the Admin credential, the
    /// same operator-only posture as `hold`.
    Erase {
        #[command(subcommand)]
        command: EraseCommand,
    },
    /// Inspect an idempotency marker object (ADR-0051 section 5).
    Idem {
        #[command(subcommand)]
        command: IdemCommand,
    },
    /// Inspect the bucket's tenant-hash scheme marker (ADR-0050 section 3).
    Tenancy {
        #[command(subcommand)]
        command: TenancyCommand,
    },
    /// Manage the durable shard_count provisioning record (ADR-0050 section 5).
    Provision {
        #[command(subcommand)]
        command: ProvisionCommand,
    },
    /// Show or set the durable deployment-wide GC configuration `sys/gc`
    /// (ADR-0050 section 4).
    GcConfig {
        #[command(subcommand)]
        command: GcConfigCommand,
    },
    /// Show or set a tenant's durable typed attribute columns for the
    /// `logs` SQL table (ADR-0090 decision 1), in
    /// `TenantConfig.typed_attr_columns` at `t/<tenant_hash>/config`. A
    /// query-serving process picks a change up within its typed-attribute-column
    /// staleness horizon; no restart is needed.
    TypedAttrColumn {
        #[command(subcommand)]
        command: TypedAttrColumnCommand,
    },
    /// Show, set or clear a tenant's clustering key (ADR-2135 decision 1),
    /// field 13 of its config record at `t/<tenant_hash>/config`.
    ClusteringKey {
        #[command(subcommand)]
        command: ClusteringKeyCommand,
    },
    /// Show or set a tenant's bloom scope (ADR-2135), field 14 of its config
    /// record at `t/<tenant_hash>/config`.
    BloomScope {
        #[command(subcommand)]
        command: BloomScopeCommand,
    },
    /// Per-tenant operator records: the deployment-wide bearer-token map
    /// `sys/auth` (ADR-0072 decision 4) and the Parquet location grants
    /// record (ADR-2040 decision D1).
    Tenant {
        #[command(subcommand)]
        command: TenantCommand,
    },
    /// Inspect, sweep and repair a tenant's Parquet table manifests (ADR-2040).
    Parquet {
        #[command(subcommand)]
        command: ParquetCommand,
    },
    /// Operate on a node's local read-cache directory (ADR-0046). A local
    /// filesystem tool: it takes no `--store` and never touches object storage.
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
    /// Bulk-import a Parquet file into the signal named by `--signal`
    /// (ADR-0089, widened to every signal by ADR-1751).
    ///
    /// Writes directly to that signal's ingest router in-process. NOTE: the
    /// per-tenant AdmissionController (active-stream cap, stream-creation rate,
    /// byte rate) that guards the HTTP ingest path is BYPASSED by construction
    /// on this path. The future-skew and length caps are enforced identically
    /// to OTLP, at the named signal's own OTLP limits; the past-event-time lag
    /// check is deliberately NOT enforced, so
    /// historical timestamps are admitted (they bucket by load time, so query
    /// with a window that reaches now). A per-record attribute cap of 1024
    /// applies, relaxed from the OTLP cap of the signal it stands in for: 64
    /// attributes per metric data point, 128 per log record, 128 per span. A
    /// metric's name and label names are rewritten exactly as OTLP rewrites
    /// them (the Prometheus character set, then the `[metrics] unit` suffix
    /// and `_total` for `kind = "counter"`), so a loaded metric lands on the
    /// same series as the same metric sent over OTLP. A loaded span is stored
    /// as the same record the same span sent over OTLP produces: the same
    /// attribute coercion, the same resource-over-span merge, the same status
    /// mapping (anything outside `0..=2` is unset), an empty or null parent
    /// cell is a root span, an attribute value over the cap drops that
    /// attribute and keeps the span (the load summary prints how many values
    /// were dropped that way), and a null attribute cell is an attribute the
    /// row does not carry, as an OTLP `KeyValue` with no value is. The
    /// complete list of what still differs
    /// for the same input: a null `start_ts`, `end_ts` or `name` cell is
    /// refused (OTLP has no null for any of them); a negative timestamp is
    /// refused (OTLP's are unsigned); a non-empty parent id of the wrong width
    /// is refused rather than dropped; and the attribute keys and the two
    /// attribute-count caps are checked against the `--mapping` before the
    /// load rather than per span, so an empty, over-long, reserved or
    /// duplicated key refuses the load where OTLP would drop or admit that one
    /// attribute, and so does a mapping declaring more than 1024
    /// `[[spans.attribute]]` columns (the loader per-record cap, standing in
    /// for OTLP's per-span cap of 128) or more than 128
    /// `[[spans.resource_attribute]]` columns (OTLP's own
    /// `max_resource_attributes`). A row that fails a
    /// kept check is
    /// rejected fail-fast: the run stops at the first bad row and exits
    /// nonzero. `--skip-rows` (issue #1713) drops that many leading rows by
    /// file-absolute position; a failed run prints the figures a resume would
    /// use. Resuming a failed load that way is sound only when it ran with
    /// `--read-cursors 1 --pipeline-depth 1`, and there is no deduplication
    /// behind it either way: see docs/guides/ingest.md for the procedure.
    /// Retention is measured from load time, not the records' event times.
    Load {
        /// Path to the source Parquet file.
        #[arg(long, value_name = "FILE")]
        parquet: std::path::PathBuf,
        /// Target tenant id (hashed under the bucket's pinned scheme).
        #[arg(long)]
        tenant: String,
        /// Path to the `--mapping` TOML (source columns to record fields).
        #[arg(long, value_name = "TOML")]
        mapping: std::path::PathBuf,
        /// Which signal to load into (ADR-1751 decision 1). The loader
        /// provisions or validates that signal, constructs its router, and
        /// writes in `WriteMode::Strict`; every ADR-0089 admission decision
        /// (past lag relaxed, future skew kept, length caps at that signal's
        /// OTLP limits, the loader attribute cap, the admission controller
        /// bypassed by construction) applies per signal. The `--mapping` file
        /// must carry exactly one signal section and it must match this flag;
        /// a mapping written before ADR-1751, whose logs keys sit at the top
        /// level, is still read as the `[logs]` section. A `[spans]` mapping
        /// names the span's ids, name, start and end timestamps with their
        /// units, optional status code and message, its resource and span
        /// attribute columns, and an optional `attrs_map_column` whose entries
        /// join the span's attributes; span events and span links are not
        /// mappable in this version and a mapping naming them is refused.
        /// Defaults to `logs`.
        #[arg(long, value_enum, default_value_t = ravel_cli::maintain::SignalArg::Logs)]
        signal: ravel_cli::maintain::SignalArg,
        /// Configured shard count. Validated against (or, for a fresh signal,
        /// written to) the durable provisioning record, exactly as the server
        /// does at first touch; the router resolves the active generation from
        /// that record. Defaults to the server's default of 4.
        #[arg(long, default_value_t = 4)]
        shards: u32,
        /// Rows per Strict flush. One flush is one RLOG object per involved
        /// shard, so on a large load this is the lever that controls how many
        /// RLOG objects the load leaves behind (a first-order query-cost
        /// variable). Must be at least 1; 0 is rejected. Defaults to
        /// `DEFAULT_BATCH_ROWS` (10000), leaving current behaviour unchanged.
        #[arg(long, default_value_t = ravel_cli::load::DEFAULT_BATCH_ROWS)]
        batch_rows: usize,
        /// Number of leading rows, by file-absolute position, to drop before
        /// mapping (issue #1713). Exact at any cursor count, and reported as
        /// `rows_skipped`. Resuming a FAILED load with `rows_skipped +
        /// rows_written` is sound only when that run used `--read-cursors 1
        /// --pipeline-depth 1`; at any other settings the rows that landed are
        /// not a prefix of the file and the offset both duplicates and drops
        /// rows. There is no idempotency marker, so nothing checks the value.
        /// See docs/guides/ingest.md. Defaults to 0 (no skip).
        #[arg(long, default_value_t = 0)]
        skip_rows: u64,
        /// Number of parallel stride read cursors over the Parquet file's row
        /// groups (issue #560). A file sorted by a resource-attribute column
        /// (e.g. ClickBench's `hits.parquet`, sorted by `CounterID`) puts one
        /// value's rows in one contiguous run, so a single sequential reader
        /// fills each `--batch-rows` batch with just that one value: one
        /// `shard_for_log` hash, one shard, no spread across `--shards`. K
        /// cursors each read a disjoint, near-even, far-apart partition of the
        /// file's row groups, and each batch is assembled from a contiguous
        /// run out of every live cursor, so one batch's rows span K different
        /// regions of the file instead of one. Omit for automatic sizing
        /// (`min(--shards, row-group count)`, floored at 1); an explicit value
        /// is clamped to `[1, row-group count]`. `1` is exactly today's
        /// sequential read. `0` is rejected. Each cursor decodes Arrow
        /// batches of `ceil(--batch-rows / K)` rows, so the decoded Arrow
        /// rows held by cursors no longer scale with K (issue #2613); each
        /// cursor still keeps its own reader and page-decode state.
        #[arg(long, value_name = "K")]
        read_cursors: Option<usize>,
        /// Number of Strict writes allowed in flight at once. Each batch's
        /// write is one S3 PUT round trip per involved shard; at depth `1` the
        /// loader submits one write and waits for its ack before building or
        /// submitting the next, so that round-trip latency is serial and the
        /// machine has nothing to run in between. Raising the depth lets up to
        /// this many writes overlap, hiding the PUT latency behind later
        /// batches' encode and I/O. Defaults to `DEFAULT_PIPELINE_DEPTH` (4),
        /// which is where the measured 2.94x on the 100M-row ClickBench corpus
        /// comes from (ADR-0807); `1` restores the old one-batch-at-a-time
        /// behavior. The cost is memory: each in-flight write keeps its built
        /// batch resident until its ack, so the window adds up to this many
        /// built batches to the working set. That is an upper bound, not the
        /// typical cost: the single decode task is usually the bottleneck and
        /// the window seldom fills, and on the 100M-row ClickBench corpus
        /// depth 4 against depth 1 was 11-17% of peak RSS at 500,000- and
        /// 1,000,000-row batches (issue #2613; docs/internal/loader-memory-2613.md
        /// attributes the peak by allocation site). The reported
        /// durable-token list is unaffected by the depth. It is always exactly
        /// the batches strictly before the failing one, in submission order,
        /// followed by whatever a batch submitted after the failing one had
        /// committed: on a failure the loader resolves every outstanding write
        /// before returning rather than abandoning it, so the report equals what
        /// landed at any depth, and a resume from it does not re-ingest rows
        /// that already committed. `0` is rejected.
        #[arg(long, default_value_t = ravel_cli::load::DEFAULT_PIPELINE_DEPTH)]
        pipeline_depth: usize,
        /// Number of flushes one shard may have in flight at once (issue #807).
        /// This bounds the shard actor's own flush pipeline, PER SHARD: the
        /// loader writes one RLOG object per batch per involved shard, and at
        /// `1` a shard actor must wait for the previous object's PUT and
        /// commit-record publish before it starts the next one, so a second
        /// batch landing on the same shard queues behind the first even when
        /// `--pipeline-depth` has already handed both to the router. The
        /// resulting ceiling on genuinely concurrent flushes is roughly
        /// `--shards` x this value, capped additionally by `--pipeline-depth`
        /// (the loader never keeps more than that many writes outstanding, so a
        /// value above `--pipeline-depth` cannot be reached). Defaults to
        /// `DEFAULT_MAX_INFLIGHT_FLUSHES`, which tracks `--pipeline-depth`'s own
        /// default (4) so the inner window never re-serialises what the outer
        /// one made concurrent. Setting it below `--pipeline-depth` makes each
        /// shard's excess batches queue on this semaphore, and they still have
        /// to clear it inside the 60s Strict ack deadline. On this bulk path it
        /// costs no extra memory: the resident flush working set is whatever the
        /// outstanding batches carry and `--pipeline-depth` already caps that,
        /// so this knob only decides whether those objects are encoded and PUT
        /// concurrently or one at a time. A Strict write's acknowledgement is
        /// unchanged by the setting: each flush answers its own waiters only
        /// after its own data object and its own commit record have landed. `1`
        /// restores one-flush-per-shard behavior. `0` is rejected: it is a
        /// semaphore no flush can ever acquire, which would deadlock the shard.
        #[arg(long, default_value_t = ravel_cli::load::DEFAULT_MAX_INFLIGHT_FLUSHES)]
        max_inflight_flushes: u32,
        /// Number of decoded batches allowed to sit queued between the Parquet
        /// decode/build stage and the shard writers (issue #680). A bounded
        /// channel decouples the two: the reader decodes batch N+1 (and, with
        /// `--read-cursors > 1`, stride-reads several row-group regions in
        /// parallel) while the encoders write batch N, so decode and encode
        /// overlap instead of running in lockstep. The reader blocks when the
        /// channel is full, so the queue holds at most this many built batches;
        /// the extra memory is roughly this count times one batch's built size,
        /// on top of `--pipeline-depth`'s in-flight-write working set. Defaults
        /// to 2. Must be at least 1; 0 is rejected.
        #[arg(long, default_value_t = ravel_cli::load::DEFAULT_DECODE_QUEUE_BATCHES)]
        decode_queue_batches: usize,
        /// Estimated in-memory bytes a shard's buffer accumulates before it
        /// flushes as one RLOG object (issue #801). At the default `1` every
        /// batch flushes as its own object the moment it is written: one object
        /// per involved shard per batch, `--batch-rows` sets its size, and no
        /// buffer lingers. A larger value lets a shard hold several batches'
        /// records in one buffer until the target is reached, so objects grow
        /// without any more Arrow batches being held in memory -- unlike
        /// raising `--batch-rows`, whose memory cost is linear because each
        /// batch is buffered whole.
        ///
        /// Two facts decide whether a given value can do anything at all
        /// (issue #971), and both bite at ClickBench scale:
        ///
        /// - The unit is the router's buffered-footprint ESTIMATE, not encoded
        ///   object bytes. Every attribute occurrence charges a 56-byte
        ///   (name, value) pair header plus its key bytes and its uncompressed
        ///   value bytes, plus the stream-attribute blob and 32 bytes per row.
        ///   For the 104-column ClickBench mapping that is roughly 8 KB per
        ///   row, while the objects the same load writes average a bit over
        ///   100 bytes per row. A target picked from an observed object size is
        ///   therefore tens of times too small to matter.
        /// - The check runs once per write, after a whole batch's per-shard
        ///   slice has merged. A target at or below one slice's estimated
        ///   footprint (`--batch-rows / --shards` rows' worth of the estimate
        ///   above) is already exceeded by the first write into an empty
        ///   buffer, so it flushes every write and reproduces the `1` layout
        ///   exactly. At `--batch-rows 40000 --shards 4` that threshold is tens
        ///   of megabytes: a target of a few MiB changes nothing, and only the
        ///   small slices a batch leaves on a lightly-hit shard accumulate at
        ///   all.
        ///
        /// A load whose `--target-bytes` turned out to lay the objects out
        /// exactly as `1` would have says so on stderr, with the threshold it
        /// missed.
        ///
        /// The trade is ack timing, not durability. A Strict write's ack is
        /// still sent only after its records' object and commit record are
        /// published, so an ack always means durable. But above `1` the flush
        /// that answers a batch's ack may be triggered by a LATER batch, so
        /// that ack now waits for one; a buffer that never reaches the target
        /// waits for the router's wall-clock age trigger instead
        /// (`--max-flush-delay`, 2s by default), or, at the end of the input,
        /// for the loader's own force-flush. Set `--pipeline-depth` to at
        /// least the number of batches that accumulate into one flush, or
        /// every flush waits out that timer. `0` is rejected.
        #[arg(long, value_name = "BYTES", default_value_t = ravel_cli::load::DEFAULT_TARGET_BYTES)]
        target_bytes: usize,
        /// How long a shard buffer may age before the router flushes it,
        /// regardless of `--target-bytes` (issue #801). A humantime duration
        /// (`2s`, `10m`, `1h5m`). Unset leaves the router's default (2s), so an
        /// omitted flag changes a load's object layout not at all.
        ///
        /// This is the THIRD binding constraint on object size, beside
        /// `--target-bytes` and one batch's per-shard slice footprint. A shard
        /// flushes on the first of: its buffer reaches `--target-bytes`, its
        /// oldest buffered point ages past this delay, or the final drain at
        /// load close. At the 2s default a buffer that fills slower than one
        /// target's worth every 2s ages out before it ever reaches a large
        /// `--target-bytes`, so the size trigger never fires and the target is
        /// unreachable as a lever no matter how large it is set: the v4 load's
        /// ~11,871-row objects are about 2s of one shard's ingest rate. To make
        /// `--target-bytes` bind, raise this past the time one target's worth
        /// takes to accumulate on a shard.
        ///
        /// The interaction is a triangle: `--target-bytes` binds only when the
        /// buffer both SURVIVES long enough (this flag) and FILLS fast enough
        /// (`--pipeline-depth` at least the number of batches that accumulate
        /// into one flush, so a later batch is in flight to push the buffer over
        /// the target before it ages out). Setting any one of the three without
        /// the other two leaves the object layout at the `--target-bytes 1`
        /// shape.
        ///
        /// The trade is ack latency, not durability, and it lands mid-load
        /// rather than at the tail. At the end of the input the loader force-
        /// flushes every shard buffer (the load report's flush mix counts those
        /// under `final`) before waiting on the last batches' acks, so a tail
        /// buffer left under `--target-bytes` is published there and never
        /// waits out this delay. Mid-load, a buffer that misses
        /// `--target-bytes` because a batch's slices fell unevenly waits up to
        /// this delay for the age trigger, and every write's ack deadline is
        /// raised to this delay plus one minute so that wait completes instead
        /// of failing the load. An ack still means durable whenever it arrives.
        ///
        /// `0s` is accepted and means the age trigger fires on the next flush
        /// tick for any non-empty buffer on this path, where every write is
        /// Strict and so leaves a waiter on the buffer it merged into (a
        /// buffer with no waiter is judged idle and ages on the router's
        /// slower idle timer instead).
        #[arg(long, value_name = "DURATION", value_parser = ravel_cli::parse_max_flush_delay)]
        max_flush_delay: Option<Duration>,
        /// The zstd level of every page and section the loader's RLOG objects
        /// compress with zstd. Compaction re-encodes the objects it merges at
        /// compaction's own level, so this sets only the load's own objects.
        ///
        /// A page or section is stored compressed only when that is smaller
        /// than storing it raw, and a page's encoding is chosen by stored
        /// size, so the level can change which encoding a page keeps. Accepts
        /// zstd's range, -131072 to 22, and refuses a level outside it. A
        /// metrics or spans load ignores this flag and warns when it is set
        /// to anything but 3.
        #[arg(
            long,
            value_name = "LEVEL",
            default_value = "3",
            allow_hyphen_values = true,
            value_parser = ravel_cli::parse_zstd_level
        )]
        zstd_level: ravel_ingest::RlogZstdLevel,
    },
    /// Bulk-export a tenant's stored logs, metrics or spans to a Parquet file (ADR-1751).
    ///
    /// The inverse of `load`: it resolves the catalog once, reads the
    /// objects that snapshot names, and writes the columns the same
    /// `--mapping` TOML describes, sorted by event time, so `load --parquet
    /// <out> --mapping <same file>` reads the file back. This is a store read,
    /// not a query: no SQL is planned and no `ravel-server` is contacted, but
    /// the objects themselves are read from object storage as usual, and the
    /// visibility rules are the query path's own, so retention tombstones,
    /// compacted-away objects, and pending selective-erasure requests exclude
    /// the same records they exclude from a query.
    ///
    /// `--start`/`--end` are a half-open event-time window `[start, end)`: a
    /// record at exactly `--end` is not exported. The whole window is held in
    /// memory before the first row is written, so export a wide range in
    /// several narrower windows.
    ///
    /// `--signal logs`, `--signal metrics` and `--signal spans` are supported.
    /// A metrics mapping the export cannot invert, such as `[metrics.histogram]`,
    /// is refused rather than written as a file that loads onto other series.
    /// A spans export writes the mapped fields, and, when the mapping sets
    /// `attrs_map_column`, the other stored attributes except the reserved
    /// ones into it, as many as the load's per-span attribute cap reads back.
    /// It refuses by name a span whose mapped fields a load would not read
    /// back as stored; a span losing what the file cannot carry is counted as
    /// `spans_with_unwritten_data`.
    Export {
        /// Signal to export: `logs`, `metrics` or `spans`. No default: a
        /// command that chooses for you which data it touches is a silent
        /// wrong answer on a tenant that holds more than one signal.
        #[arg(long, value_enum)]
        signal: SignalArg,
        /// Source tenant id (hashed under the bucket's pinned scheme).
        #[arg(long)]
        tenant: String,
        /// Inclusive start of the event-time window, RFC 3339 with `Z` or a
        /// numeric offset, which is converted to UTC
        /// (`2024-01-01T00:00:00Z`, `2024-01-01T02:00:00+02:00`).
        #[arg(long, value_name = "RFC3339", value_parser = ravel_cli::parse_rfc3339_ns)]
        start: i64,
        /// Exclusive end of the event-time window, RFC 3339 with `Z` or a
        /// numeric offset, which is converted to UTC. Must be after
        /// `--start`.
        #[arg(long, value_name = "RFC3339", value_parser = ravel_cli::parse_rfc3339_ns)]
        end: i64,
        /// Path of the Parquet file to write. Replaced only once the export
        /// finishes: the rows go to a temporary file beside it which is
        /// renamed over it at the end, so a failed export leaves an existing
        /// file untouched. The rename replaces a symlink itself rather than
        /// the file it points to, and does not keep the old file's mode,
        /// owner or ACLs. A directory, any other non-regular file, and any
        /// path under `/dev` are refused before the export reads anything.
        #[arg(long, value_name = "FILE")]
        parquet: std::path::PathBuf,
        /// Path to the `--mapping` TOML naming the output columns. The same
        /// file a `load` of this data used produces a file that load reads
        /// back, or, for metrics, refuses by name a series no file under it
        /// re-loads as the same one. For spans, every mapped field round-trips,
        /// a span whose mapped fields would not is refused by name,
        /// `attrs_map_column` carries the unreserved attributes the mapping
        /// does not name up to the load's per-span attribute cap, and a span
        /// losing what the file cannot carry is counted as
        /// `spans_with_unwritten_data`.
        #[arg(long, value_name = "TOML")]
        mapping: std::path::PathBuf,
        /// Configured shard count, used to resolve the catalog. The tenant's
        /// durable provisioning record supplies the real per-hour shard
        /// generations on top of it. Defaults to the server's default of 4.
        #[arg(long, default_value_t = 4)]
        shards: u32,
        /// The deployment's `ravel-server --max-ingest-lag` (humantime
        /// duration, e.g. `6h`). The catalog lists ingest-hour buckets from
        /// `--start` minus this value forward, which is what reaches the
        /// bucket of a record whose event time falls in a later ingest hour
        /// than the bucket it was written into. Defaults to the server's own
        /// 2h default; pass the server's value when the deployment differs,
        /// or the export resolves a different window than a query over the
        /// same range. Zero is refused, as the server refuses it.
        #[arg(long, value_name = "DURATION", value_parser = ravel_cli::parse_max_ingest_lag_ns)]
        max_ingest_lag: Option<i64>,
    },
}

#[derive(Debug, Subcommand)]
enum CacheCommand {
    /// Reclaim pre-namespacing local disk-cache files (issue #826).
    ///
    /// `DiskCache` used to write entry files at `<cache-dir>/<shard>/<file>`;
    /// since the per-instance namespace layout (issue #671) it writes under
    /// `<cache-dir>/<namespace>/<shard>/<file>`. Files left at the old
    /// rootless layout are inert: never seeded, never evicted, counted in no
    /// budget, so a directory warmed before that upgrade keeps up to the old
    /// budget on disk forever. Nothing deletes them implicitly; this is the
    /// deliberate operator step that does.
    ///
    /// Safe to run while a node is live: no live code path reads or writes the
    /// legacy `<cache-dir>/<shard>` layout, so deleting it races nothing.
    ///
    /// Dry run by default: it prints the legacy files it would delete and their
    /// total bytes, and deletes nothing. Pass `--apply` to delete them.
    ///
    /// Downgrade story: a binary rolled back to the pre-namespacing layout
    /// reads `<cache-dir>/<shard>` again and finds whatever reclaim left. After
    /// `--apply` it finds nothing there and starts with an empty disk cache,
    /// which is a cold start (the next reads refetch from object storage), not
    /// data loss: the cache is disposable by construction.
    ReclaimLegacy {
        /// The node's configured read-cache directory (the server's
        /// `--read-cache-dir`).
        #[arg(long, value_name = "DIR")]
        cache_dir: std::path::PathBuf,
        /// Delete the legacy files. Without this, the command only lists them.
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Debug, Subcommand)]
enum TenantCommand {
    /// Manage bearer tokens in `sys/auth`.
    Token {
        #[command(subcommand)]
        command: TenantTokenCommand,
    },
    /// Manage the tenant's Parquet location grants (ADR-2040 decision D1):
    /// the external locations this tenant may define Parquet tables over.
    ParquetGrant {
        #[command(subcommand)]
        command: TenantParquetGrantCommand,
    },
}

#[derive(Debug, Subcommand)]
enum TenantParquetGrantCommand {
    /// Grant one location to one credential profile, after qualifying the
    /// store behind it.
    ///
    /// Refuses a location whose scheme does not address the profile's store
    /// kind; a prefix holding no object, which leaves nothing to probe;
    /// a store that serves a read carrying an ETag it never issued, which
    /// cannot pin a Parquet file; and a bucket that is Ravel's own reached
    /// under another name, or whose answer about that is inconclusive.
    Add {
        /// The tenant to grant the location to.
        #[arg(long)]
        tenant: String,
        /// The location URL: `s3://bucket/prefix`, `gs://...` or `az://...`.
        /// A trailing `/` names a set of objects.
        #[arg(long)]
        location: String,
        /// The credential profile name, resolved in the file named by the
        /// top-level `--parquet-profiles`.
        #[arg(long)]
        profile: String,
    },
    /// Revoke the grant that is exactly this location. A location merely
    /// admitted by a wider grant is not removed: name the grant itself.
    Remove {
        /// The tenant to revoke the grant from.
        #[arg(long)]
        tenant: String,
        /// The granted location URL, exactly as it was granted.
        #[arg(long)]
        location: String,
    },
    /// Print every field of every grant this tenant holds.
    Ls {
        /// The tenant whose grants to print.
        #[arg(long)]
        tenant: String,
    },
}

#[derive(Debug, Subcommand)]
enum ParquetCommand {
    /// Print every manifest field of each table's newest version. With
    /// `--table`, print every retained version of that one table at or below
    /// the version bound instead.
    Ls {
        /// The tenant whose Parquet tables to print.
        #[arg(long)]
        tenant: String,
        /// Restrict the output to one table, and print every version of it
        /// at or below the version bound that has not been swept.
        #[arg(long)]
        table: Option<String>,
    },
    /// Delete manifest versions superseded for longer than `--grace`.
    ///
    /// Deletes nothing outside `t/<tenant_hash>/pq/t/`: the Parquet files a
    /// table names live in the tenant's own bucket and a sweep never touches
    /// them. A grace below the deployment's stored `max_query_duration`
    /// (`sys/gc`) is refused. That floor protects a writer's resolve-to-put
    /// window: a writer finishes its put within half of the floor after its
    /// resolve, so while it is in flight no sweep can free the version key
    /// its create-if-absent put targets. A query needs no protection here,
    /// since it reads a table's manifest once, when it resolves.
    Sweep {
        /// The tenant whose superseded manifest versions to delete.
        #[arg(long)]
        tenant: String,
        /// How long a superseded version is kept, as a humantime duration
        /// (`1h`, `90m`). Must be at least the deployment's stored
        /// `max_query_duration`.
        #[arg(long)]
        grace: String,
    },
    /// The repair for a forged manifest version: list one table's manifest
    /// version keys and flag those above the version bound and those naming
    /// no version; with `--delete`, delete exactly the flagged ones; with
    /// `--delete-version N`, delete exactly version N.
    ///
    /// No DDL statement writes a version above the bound (2^32) or a `.pqm`
    /// key under a table's `v/` prefix whose slot is not a version number, so
    /// one there was put directly in the bucket, for example with a stolen
    /// Query credential. Readers and
    /// the sweep already skip it; `--delete` removes it. A forged version at
    /// or below the bound is not flagged: as the newest it serves as the
    /// table, and exactly at the bound it blocks every later DDL. Remove one
    /// with `--delete-version` once the DDL audit log shows no statement wrote
    /// it. Prints each key, when the store wrote it, and its `created_by` and
    /// `statement` (reported unreadable when the credential may not read
    /// manifests), and marks the flagged ones. Without `--delete` or
    /// `--delete-version` nothing is deleted. Run it under the Maintain
    /// credential, the only role that may delete manifest versions.
    ///
    /// With `--stray` instead of `--table`, list every `.pqm` key under the
    /// tenant's `t/<tenant_hash>/pq/t/` whose segment before `/v/` is not a
    /// valid table name (upper case, reserved, or a path such as `a/b`), which
    /// the Query grant also admits and the tenant-wide listings skip; with
    /// `--delete`, delete those, then list again and fail naming any still
    /// there. A key the S3 adapter would send a delete of to a different key
    /// (one holding a character its path encoding escapes, such as `~` or
    /// `%`) is marked undeletable and skipped, and a key under a name reserved
    /// after tables could be created (such as `l0`) is marked as possibly a
    /// table created before the reservation and skipped unless
    /// `--include-reserved-names` is passed. On S3 a key holding a control
    /// character, an empty segment or a `.` or `..` segment fails this listing
    /// and the tenant-wide ones; delete that exact key with the Maintain
    /// credential through an S3 tool.
    Repair {
        /// The tenant that owns the table.
        #[arg(long)]
        tenant: String,
        /// The table whose manifest versions to list.
        #[arg(long, required_unless_present = "stray", conflicts_with = "stray")]
        table: Option<String>,
        /// List the tenant's keys under no valid table name instead of one
        /// table's versions.
        #[arg(long)]
        stray: bool,
        /// Delete every flagged key, or with `--stray` every listed key not
        /// marked as skipped. Without it or `--delete-version` the command
        /// only lists.
        #[arg(long, conflicts_with = "delete_version")]
        delete: bool,
        /// Delete exactly this version's key, from 1 to the version bound,
        /// and nothing else. For a version judged forged from the DDL audit
        /// log; zero and versions above the bound are refused.
        #[arg(long, value_name = "N", conflicts_with = "stray")]
        delete_version: Option<u64>,
        /// With `--stray --delete`, also delete keys under a name reserved
        /// after tables could be created, which may be manifests of a table
        /// created before the reservation.
        #[arg(
            long,
            requires = "stray",
            requires = "delete",
            conflicts_with = "table"
        )]
        include_reserved_names: bool,
    },
}

#[derive(Debug, Subcommand)]
enum TenantTokenCommand {
    /// Map a bearer token to a tenant, hashing it under the deployment key.
    /// The plaintext is hashed and dropped, never persisted.
    Upsert {
        /// Path to the bucket's 32-byte deployment key (64 hex characters or
        /// 32 raw bytes); the same key used for `--tenant-hash-key-file`.
        #[arg(long, value_name = "PATH")]
        deployment_key_file: std::path::PathBuf,
        /// The bearer token, in the clear. Prefer a shell mechanism that
        /// avoids process-list/history exposure (e.g. `--token "$(cat f)"`).
        #[arg(long)]
        token: String,
        /// The tenant this token authenticates as.
        #[arg(long)]
        tenant: String,
        /// Which writer owns this entry's lifecycle, stamped onto it
        /// (ADR-0072 decision 4 amendment). The operator's
        /// reconcile loop only ever removes or replaces entries tagged
        /// "operator"; anything else (the "cli" default, or a caller's own
        /// tag) is never touched by an operator reconcile.
        #[arg(long, default_value = "cli")]
        managed_by: String,
    },
    /// Remove every token mapped to a tenant. Needs no plaintext token:
    /// entries carry the tenant id in the clear, so this is correct even when
    /// the caller has never seen the tenant's tokens.
    Revoke {
        /// Path to the bucket's 32-byte deployment key (64 hex characters or
        /// 32 raw bytes); the same key used for `--tenant-hash-key-file`.
        #[arg(long, value_name = "PATH")]
        deployment_key_file: std::path::PathBuf,
        /// The tenant to revoke every token for.
        #[arg(long)]
        tenant: String,
    },
    /// List every entry's tenant id and a short token fingerprint. Never
    /// prints a raw token hash or plaintext.
    List {
        /// Path to the bucket's 32-byte deployment key (64 hex characters or
        /// 32 raw bytes); the same key used for `--tenant-hash-key-file`.
        #[arg(long, value_name = "PATH")]
        deployment_key_file: std::path::PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ClusteringKeyCommand {
    /// Print the tenant's clustering key: never set, absent at a generation,
    /// or set, with its columns and their declared types, bucket width and
    /// generation. A stored key the record's validation refuses is an error.
    Show {
        /// The tenant whose clustering key to print.
        #[arg(long)]
        tenant: String,
    },
    /// Set the tenant's clustering key at the stored clustering generation
    /// plus one, writing a version-3 config record. Every column must be a
    /// typed attribute column the tenant's config record itself declares, at
    /// most four, each named once; a refused key writes nothing. Log objects
    /// flushed after an ingest process reads the new record sort by
    /// (stream, time bucket, key columns, timestamp). Objects already written
    /// keep their order and their filters until compaction rewrites them: L1
    /// compaction and the erasure rewrite take the sort descriptor and bloom
    /// coverage of the input with the highest generation, re-sort every part
    /// by that descriptor, and compress at zstd level 9.
    Set {
        /// The tenant whose clustering key to set.
        #[arg(long)]
        tenant: String,
        /// A key column, in key order; repeat or list several.
        #[arg(long = "column", value_name = "NAME", required = true, num_args = 1..)]
        columns: Vec<String>,
        /// The time bucket width that leads the key.
        #[arg(long, value_enum)]
        bucket_width: ravel_cli::storage_layout::BucketWidthArg,
        #[command(flatten)]
        rollout: ReadersRolledOutArg,
    },
    /// Clear the tenant's clustering key at the stored clustering generation
    /// plus one, keeping the generation in a version-3 config record. Refused,
    /// writing nothing, when the tenant has no key to clear.
    Clear {
        /// The tenant whose clustering key to clear.
        #[arg(long)]
        tenant: String,
        #[command(flatten)]
        rollout: ReadersRolledOutArg,
    },
}

#[derive(Debug, Subcommand)]
enum BloomScopeCommand {
    /// Print the tenant's bloom scope: all (the default, and what a record
    /// without the field reads as), undeclared, or text.
    Show {
        /// The tenant whose bloom scope to print.
        #[arg(long)]
        tenant: String,
    },
    /// Set the tenant's bloom scope, writing a version-3 config record. A
    /// change increments the clustering generation and leaves the clustering
    /// key as it is; the scope already stored writes nothing.
    Set {
        /// The tenant whose bloom scope to set.
        #[arg(long)]
        tenant: String,
        /// Which string columns get bloom filters.
        #[arg(long, value_enum)]
        scope: ravel_cli::storage_layout::BloomScopeArg,
        #[command(flatten)]
        rollout: ReadersRolledOutArg,
    },
}

/// The storage-layout write opt-in every clustering-key and bloom-scope write
/// requires (ADR-0066 R1); without it the command exits 2 before any store
/// request.
#[derive(Debug, clap::Args)]
struct ReadersRolledOutArg {
    /// Assert that every process reading this bucket's tenant config runs a
    /// release that reads record version 3; a process that does not refuses
    /// the record, and that tenant's ingest and lifecycle fail closed.
    #[arg(long, required = true)]
    readers_rolled_out: bool,
}

impl ReadersRolledOutArg {
    fn write(&self) -> ravel_catalog::StorageLayoutWrite {
        if self.readers_rolled_out {
            ravel_catalog::StorageLayoutWrite::ReadersRolledOut
        } else {
            ravel_catalog::StorageLayoutWrite::Disabled
        }
    }
}

#[derive(Debug, Subcommand)]
enum TypedAttrColumnCommand {
    /// Print the tenant's durable declaration, or report that it is unset (in
    /// which case the deployment default, from ravel-server's
    /// `--typed-attr-column` flags, applies).
    Show {
        /// The tenant whose declaration to print.
        tenant: String,
    },
    /// Replace the tenant's declaration wholesale, validating it first and
    /// swapping the record with `CasVersion` so a concurrent write is a
    /// reported conflict rather than a silent overwrite. Not additive and with
    /// no per-key remove: pass the full intended list. Passing no declaration
    /// writes an explicit empty one, which means "this tenant declares nothing"
    /// and is distinct from having no override at all.
    Set {
        /// The tenant whose declaration to replace.
        tenant: String,
        /// The declaration, as `KEY:TYPE` specs in schema-append order, where
        /// TYPE is one of str/i64/bool/bytes (case-insensitive). A key may
        /// contain `:`; the type is split off the right. Mutually exclusive
        /// with `--from-mapping`.
        #[arg(value_name = "KEY:TYPE", conflicts_with = "from_mapping")]
        columns: Vec<String>,
        /// Derive the declaration from a `load --mapping` TOML instead of
        /// positional `KEY:TYPE` specs: every `[[attribute]]` and
        /// `[[resource_attribute]]` entry becomes a typed attribute column of the
        /// same-named type. `f64`-typed entries are skipped with a per-key
        /// warning on stderr (no typed attribute column can be `f64`); the rest
        /// are written through the same CAS whole-list replace. Mutually
        /// exclusive with positional `KEY:TYPE` specs.
        #[arg(long, value_name = "TOML")]
        from_mapping: Option<std::path::PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum GcConfigCommand {
    /// Print the durable `sys/gc` values (format version, protection horizon,
    /// grace, max query duration, max flush lifetime, HEAD cache TTL), or
    /// report that the bucket is not yet bootstrapped.
    Show {},
    /// Write a full new `sys/gc`, enforcing `protection_horizon >=
    /// max_query_duration + grace + clock_skew_allowance` and
    /// `protection_horizon >= max_compaction_lifetime + 4 *
    /// clock_skew_allowance` (this build's compiled 1h compaction lifetime) at
    /// write time and swapping the durable object with `CasVersion`. All
    /// durations are humantime strings (e.g. `25h5m`).
    Set {
        /// Horizon between a deletion anchor and physical deletion (e.g.
        /// `25h5m`).
        #[arg(long, value_name = "DURATION")]
        protection_horizon: String,
        /// Shared grace period for the GC age gates (e.g. `24h`).
        #[arg(long, value_name = "DURATION")]
        grace: String,
        /// Longest a single query may run (e.g. `1h`).
        #[arg(long, value_name = "DURATION")]
        max_query_duration: String,
        /// Longest a flush may stay open (e.g. `1h`). Refused below the
        /// ingest pipeline's own default `max_flush_lifetime` (currently
        /// `1h`): a lower value here lets GC seal a bucket before an
        /// in-flight flush's writer interlock expires.
        #[arg(long, value_name = "DURATION")]
        max_flush_lifetime: String,
        /// Cross-host clock-skew allowance the horizon must cover (e.g. `5m`).
        /// The constraint input that closes S1-02; must match the sweepers'
        /// `clock_skew_allowance`. Not stored in `sys/gc`. Defaults to 5m when
        /// omitted.
        #[arg(long, value_name = "DURATION")]
        clock_skew_allowance: Option<String>,
        /// HEAD cache TTL every query-mode server process is held to (e.g.
        /// `30s`). Writes `sys/gc` format version 2 recording it (ADR-1133):
        /// run it only once every `ravel-server` process in every mode
        /// (gateway, query, maintain, all) and every `ravel-cli` binary that
        /// reads `sys/gc` runs a build that reads version 2, since an older
        /// build then refuses the object. The flip is one-way. Omitted, the
        /// stored format version is kept, and a stored version 2 keeps its
        /// recorded TTL.
        #[arg(long, value_name = "DURATION")]
        head_cache_ttl: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ProvisionCommand {
    /// Adopt pre-ADR data into a `shard_count` provisioning record, ahead of a
    /// server touching the tenant (ADR-0050 section 5). Runs the same adoption
    /// path the server runs at ingest/maintenance: writes the record only when
    /// every observed shard index is below `--shards`, and refuses (writing
    /// nothing) when a higher shard index proves the value would hide data.
    Adopt {
        /// Tenant id (hashed under the bucket's pinned scheme).
        #[arg(long)]
        tenant: String,
        /// The configured shard_count to adopt at (the server's `--shards`).
        #[arg(long)]
        shards: u32,
        /// Restrict to one signal; omit to adopt metrics, logs, and spans.
        #[arg(long, value_enum)]
        signal: Option<SignalArg>,
    },
    /// Reshard a (tenant, signal) online (ADR-0052): append a new shard
    /// generation to its provisioning record under CasVersion and write a
    /// control-plane audit record. Existing data is never moved or re-keyed;
    /// only future data (from the activation hour onward) routes with the new
    /// count. The activation is placed `--lead-hours` in the future, which must
    /// be at least ceil(C) + 1 = 2 hours so every live writer observes the new
    /// generation before it activates or fail-stops on record staleness.
    Reshard {
        /// Tenant id (hashed under the bucket's pinned scheme).
        #[arg(long)]
        tenant: String,
        /// The signal to reshard.
        #[arg(long, value_enum)]
        signal: SignalArg,
        /// The new shard_count for the appended generation (1..=10000).
        #[arg(long)]
        shard_count: u32,
        /// Hours ahead of now to activate the new generation. Must be >= 2
        /// (ceil(C) + 1 with the default 60s refresh interval C). Defaults to 2.
        #[arg(long, default_value_t = 2)]
        lead_hours: u32,
    },
}

#[derive(Debug, Subcommand)]
enum HoldCommand {
    /// Place a legal hold, writing an immutable ADR-0040 audit record. Either
    /// `--scope` alone, or `--signal` together with `--shard` (the sugar,
    /// which writes all three `shard_hold_scopes` prefixes).
    Set {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        scope: Option<String>,
        #[arg(long, value_enum)]
        signal: Option<SignalArg>,
        #[arg(long)]
        shard: Option<u32>,
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Release a legal hold, writing an immutable ADR-0040 audit record. Same
    /// `--scope` or `--signal`/`--shard` sugar as `hold set`.
    Clear {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        scope: Option<String>,
        #[arg(long, value_enum)]
        signal: Option<SignalArg>,
        #[arg(long)]
        shard: Option<u32>,
    },
    /// List a tenant's currently active legal holds.
    List {
        #[arg(long)]
        tenant: String,
    },
}

#[derive(Debug, Subcommand)]
enum EraseCommand {
    /// Submit an immutable erasure request: a conjunction of exact-match
    /// label/attribute matchers plus an optional event-time window, and an
    /// optional free-text reason. Written `.dreq` with CreateIfAbsent; prints
    /// the assigned request_id. A request id is generated unless `--request-id`
    /// is given (supply it to retry a prior submit idempotently).
    Submit {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        /// Exact-match predicate matcher `key=value`, repeatable; the request
        /// matches a record only when every matcher holds (logical AND). At
        /// least one is required.
        #[arg(long = "matcher", value_name = "KEY=VALUE", required = true)]
        matchers: Vec<String>,
        /// Optional inclusive event-time window start (unix ns). Both bounds
        /// zero (the default) means no event-time restriction.
        #[arg(long, default_value_t = 0)]
        window_start_ns: i64,
        /// Optional exclusive event-time window end (unix ns).
        #[arg(long, default_value_t = 0)]
        window_end_ns: i64,
        /// Optional free-text operator reason.
        #[arg(long, default_value = "")]
        reason: String,
        /// Reuse an explicit request id (UUID) instead of generating one, to
        /// retry a prior submit idempotently under CreateIfAbsent.
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Report an erasure request's state: pending (a `.dreq`, no `.done`),
    /// completed (a `.done`, with per-bucket dropped counts and any deferral
    /// cause), or unknown. Omit `--request-id` to list every request for the
    /// (tenant, signal).
    Status {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum IdemCommand {
    /// Fetch and decode an idempotency marker by its exact object key
    /// (`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`).
    Inspect {
        /// Object store key of the marker.
        key: String,
    },
}

#[derive(Debug, Subcommand)]
enum TenancyCommand {
    /// Print the bucket's `sys/tenancy` marker: its scheme and, for a keyed
    /// bucket, the key fingerprint. With `--tenant-hash-key-file`, also
    /// derives that key's fingerprint and reports whether it matches the
    /// marker (the same wrong-key check the server makes at startup, offline).
    Show {
        /// Optional 32-byte deployment key file (64 hex chars or 32 raw
        /// bytes) to verify against the marker's fingerprint.
        #[arg(long, value_name = "PATH")]
        tenant_hash_key_file: Option<std::path::PathBuf>,
    },
    /// Hash a tenant id under the bucket's resolved scheme and print its
    /// object-store prefix (`t/<hash>/`) on stdout and nothing else (issue
    /// #1180): the one entry point for turning a tenant id into the prefix a
    /// bytes-summed total, a lifecycle rule, or an erasure check needs,
    /// instead of scraping it off a failure message. Exits nonzero when the
    /// bucket's marker is `v2-keyed` and the global `--tenant-hash-key-file`
    /// was not given: that flag supplies the key the derivation needs. The
    /// global `--tenant-hash-unkeyed` is not an alternative way to satisfy a
    /// keyed marker: it selects the `v1-unkeyed` derivation, and is itself
    /// rejected against a keyed bucket.
    Resolve {
        /// Tenant id to resolve (hashed under the bucket's pinned scheme).
        tenant: String,
    },
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Run the conformance suite against the configured backend and, on a
    /// pass, record the outcome at `sys/qualification`.
    Qualify {
        /// List page size to build the store with and declare to the
        /// conformance suite's listing probes. Defaults to the production S3
        /// page size, so a default run proves a real continuation-token
        /// boundary is crossed; must match the store this command builds, so
        /// the cross-page probe judges a real pagination boundary rather than
        /// a mismatched, meaningless one. The upper bound is the number of
        /// objects a run would write: each listing probe puts the page size
        /// plus two scratch objects into the bucket, so a page size beyond a
        /// million is a typo that would fill a bucket, not a page size any
        /// backend serves.
        #[arg(
            long,
            default_value_t = ravel_object_store::s3::LIST_PAGE_SIZE,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=1_000_000)
        )]
        list_page_size: usize,
    },
    /// Read the bucket's protection configuration and check it against the
    /// deployment's expectations: one line per condition, then a summary. Exits
    /// 0 only when every expected condition passes, 1 when any fails, and 2
    /// when any could not be verified or the bucket's control plane could not
    /// be reached. A usage error (a missing or malformed flag) also exits 2,
    /// before anything is read, and so does a report that could not be written
    /// to stdout (a closed pipe excepted). `object-retention` is not checked
    /// by this command: it is printed as not checked and does not affect the
    /// exit code. Read-only.
    VerifyProtection {
        /// The noncurrent-version expiration, in days, the lifecycle rule
        /// covering `t/` must carry (`E_v`).
        #[arg(long, value_name = "DAYS")]
        expected_noncurrent_days: u32,
        /// Expect replication: `delete-marker-replication` must pass. Without
        /// it the condition is printed and does not affect the exit code.
        #[arg(long)]
        expect_replication: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SegmentCommand {
    Inspect {
        /// Local file path or object store key.
        path: String,
    },
}

#[derive(Debug, Subcommand)]
enum InspectCommand {
    /// Decode a column-statistics (`.cstat`) object's envelope and header
    /// (ADR-0850/ADR-0942/ADR-1413), and report whether its declared
    /// `body_uncompressed_len` exceeds the decode ceiling, without ever
    /// decompressing the body.
    Cstat {
        /// Local file path or object store key.
        key: String,
    },
}

#[derive(Debug, Subcommand)]
enum RlogCommand {
    Inspect {
        /// Local file path or object store key.
        path: String,
    },
    /// Attribute every stored byte of RLOG objects to section, column and
    /// encoding. Reads each object's trailer, footer, FIELD_DIR and PAGE_DIR
    /// by range, never its page bodies.
    Footprint {
        /// Measure every logs data object the catalog resolves for this
        /// tenant over all time (live L0 flush and L1 compacted segments).
        #[arg(long, conflicts_with = "objects", required_unless_present = "objects")]
        tenant: Option<String>,
        /// Shard count for the `--tenant` catalog resolve.
        #[arg(long, default_value_t = 4, requires = "tenant")]
        shards: u32,
        /// Print the report as one JSON document.
        #[arg(long)]
        json: bool,
        /// Local file paths or object store keys to measure.
        objects: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum RspanCommand {
    Inspect {
        /// Local file path or object store key.
        path: String,
    },
}

#[derive(Debug, Subcommand)]
enum CommitCommand {
    Decode {
        /// Local file path or object store key.
        key: String,
    },
    /// Decode and print a CompactionRecord (proto).
    DecodeCompaction {
        /// Local file path or object store key.
        key: String,
    },
    /// Decode and print a RetentionTombstone (proto).
    DecodeTombstone {
        /// Local file path or object store key.
        key: String,
    },
    /// Reconstruct lost L0 commit records for one shard from the record-less
    /// data objects' own footers (ADR-0058 decision 2). Scoped to
    /// a single (tenant, signal, shard) to bound blast radius. Writes
    /// CreateIfAbsent only, never overwrites or deletes an existing record;
    /// exits nonzero if any candidate failed.
    Reconstruct {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long)]
        shard: u32,
    },
}

#[derive(Debug, Subcommand)]
enum MaintainCommand {
    /// Run one compaction pass over a single sealed bucket. A bucket with at
    /// least 64 MiB of input is first claimed (ADR-1029); if another process
    /// holds its claim, it is reported as ClaimSkipped with the holder and not
    /// merged.
    CompactBucket {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long)]
        shard: u32,
        #[arg(long)]
        hour: u32,
        /// Compute the plan and report it, but write no L1 segments or record.
        /// Takes no compaction claim.
        #[arg(long)]
        dry_run: bool,
        /// Take no compaction claim, for repair work when a claim is in the
        /// way. Safe between two compactions, because the compaction record's
        /// create-if-absent still decides which output is published, though
        /// the merge may duplicate one another maintainer is running. Against
        /// an erasure rewrite of the same bucket only the pre-publish re-list
        /// then fences the publish, which leaves a short window between the
        /// re-list and the record PUT.
        #[arg(long)]
        no_claim: bool,
        /// Override the compactor's `max_flush_lifetime` (humantime duration,
        /// e.g. `30m`, `0s`; the same grammar and unit as ravel-server's
        /// `--gc-max-flush-lifetime`). A bucket seals only at its hour's end
        /// plus this plus the clock-skew allowance, so lowering it seals
        /// buckets sooner. UNSAFE below the ingest path's real flush lifetime:
        /// a bucket a writer is still flushing into can then be sealed and
        /// compacted, and that writer's later-published object is missed by the
        /// compaction. The default is the safe 1h; use this only for a tenant
        /// known quiescent, such as one whose bulk load has finished.
        #[arg(long, value_name = "DURATION",
              value_parser = parse_max_flush_lifetime_ns)]
        max_flush_lifetime: Option<i64>,
        /// The decoded record-heap size at which a merge closes an in-progress
        /// L1 segment (a split target, not a peak-memory bound: a span merge
        /// can overshoot it by a whole trace). Applies to log and span merges.
        /// Refused at 0. Default for log merges: derived from the host's
        /// memory (MemTotal capped by a cgroup memory limit) less an overhead
        /// reserve (2 GiB from 8 GiB of memory up, below it a quarter of that memory
        /// but at least 256 MiB) and the 20 GiB merge cursor budget, divided by 8,
        /// capped at the part size the claim lease supports (1500 MiB at the
        /// default 300 s lease) and at 8 GiB, with a 256 MiB floor applied
        /// last; 256 MiB when the host memory cannot be read. A derived value
        /// also becomes the log stored-size cap unless --max-l1-part-bytes is
        /// given, so log parts close on this target. Default for span merges:
        /// 256 MiB. Part boundaries depend on it, so two runs over one bucket
        /// cut the same parts only at the same value.
        #[arg(long, value_name = "BYTES")]
        l1_part_memory_target_bytes: Option<u64>,
        /// Bound the encoded/on-object bytes a log or metrics merge writes
        /// before it closes an L1 segment (the stored-size target). A log
        /// segment closes on whichever of this and the log memory target is
        /// reached first; a metrics segment closes on this alone and never
        /// reads the memory target; span merges do not read this. Refused at 0.
        /// Setting it sets the log and metrics caps together. Unset, the
        /// metrics cap is 256 MiB and the log cap follows the derived log
        /// memory target; an explicit --l1-part-memory-target-bytes above
        /// 256 MiB leaves the log cap at 256 MiB, so the log merge then runs
        /// exact-encode probes and can close a part on stored size.
        #[arg(long, value_name = "BYTES")]
        max_l1_part_bytes: Option<u64>,
        /// The zstd level an RLOG compaction writes its L1 segments at. Higher
        /// levels store smaller segments for more compaction CPU; reads decode
        /// any level the same way. Refused outside 1..=22. Default 9 (the
        /// compactor default).
        #[arg(long, value_name = "LEVEL")]
        compaction_zstd_level: Option<i32>,
        #[command(flatten)]
        tenant_kms: store::TenantKmsArgs,
    },
    /// Compact every sealed bucket of a whole tenant signal: walk each shard's
    /// ingest hours and run the same per-bucket compaction `compact-bucket`
    /// runs, so an operator no longer has to guess the hour numbers or write a
    /// per-(shard, hour) shell loop. Each bucket with at least 64 MiB of input
    /// is claimed before its merge (ADR-1029); a bucket another process holds
    /// the claim on is reported as ClaimSkipped with the holder, counted in
    /// claim_skipped, and not merged.
    CompactTenant {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        /// Shard count to walk (shards `0..N`). Omit to resolve it from the
        /// tenant's durable shard-count provisioning record; given together
        /// with a record, the two must agree. With neither flag nor record the
        /// command errors, naming the tenant.
        #[arg(long)]
        shards: Option<u32>,
        /// First ingest-hour bucket to consider, inclusive. Omit to start at
        /// each shard's oldest present hour.
        #[arg(long)]
        from_hour: Option<u32>,
        /// Last ingest-hour bucket to consider, inclusive. Omit to stop at the
        /// current hour.
        #[arg(long)]
        to_hour: Option<u32>,
        /// Compute each bucket's plan and report it, but write no L1 segments or
        /// records. Takes no compaction claims.
        #[arg(long)]
        dry_run: bool,
        /// Take no compaction claims, for repair work when a claim is in the
        /// way. Safe between two compactions, because each compaction record's
        /// create-if-absent still decides which output is published, though a
        /// merge may duplicate one another maintainer is running. Against an
        /// erasure rewrite of the same bucket only the pre-publish re-list then
        /// fences each publish, which leaves a short window between the
        /// re-list and the record PUT.
        #[arg(long)]
        no_claim: bool,
        /// Override the compactor's `max_flush_lifetime` (humantime duration,
        /// e.g. `30m`, `0s`; the same grammar and unit as ravel-server's
        /// `--gc-max-flush-lifetime`). A bucket seals only at its hour's end
        /// plus this plus the clock-skew allowance, so lowering it seals
        /// buckets sooner. UNSAFE below the ingest path's real flush lifetime:
        /// a bucket a writer is still flushing into can then be sealed and
        /// compacted, and that writer's later-published object is missed by the
        /// compaction. The default is the safe 1h; use this only for a tenant
        /// known quiescent, such as one whose bulk load has finished.
        #[arg(long, value_name = "DURATION",
              value_parser = parse_max_flush_lifetime_ns)]
        max_flush_lifetime: Option<i64>,
        /// The decoded record-heap size at which a merge closes an in-progress
        /// L1 segment (a split target, not a peak-memory bound: a merge can
        /// overshoot it, e.g. by a whole trace on the RSPAN path, so size the
        /// host for path-specific overshoot). Lower it for smaller segments on a
        /// small host; raise it for fewer, larger segments. Applies to log and
        /// span merges. Refused at 0. Default for log merges: derived from the
        /// host's memory (MemTotal capped by a cgroup memory limit) less an
        /// overhead reserve (2 GiB from 8 GiB of memory up, below it a quarter
        /// of that memory but at least 256 MiB) and the 20 GiB merge cursor budget (shared
        /// by the concurrent buckets), divided by 8 and by --bucket-concurrency,
        /// capped at the part size the claim lease supports (1500 MiB at the
        /// default 300 s lease) and at 8 GiB, with a 256 MiB floor applied
        /// last; 256 MiB when the host memory cannot be read. A derived value
        /// also becomes the log stored-size cap unless --max-l1-part-bytes is
        /// given. Default for span merges: 256 MiB. The report prints each
        /// value, where it came from and which term bound it.
        #[arg(long, value_name = "BYTES")]
        l1_part_memory_target_bytes: Option<u64>,
        /// Bound the encoded/on-object bytes a log or metrics merge writes
        /// before it closes an L1 segment (the stored-size target). A log
        /// segment closes on whichever of this and the log memory target is
        /// reached first; a metrics segment closes on this alone and never
        /// reads the memory target; span merges do not read this. Refused at 0.
        /// Setting it sets the log and metrics caps together. Unset, the
        /// metrics cap is 256 MiB and the log cap follows the derived log
        /// memory target; an explicit --l1-part-memory-target-bytes above
        /// 256 MiB leaves the log cap at 256 MiB, so the log merge then runs
        /// exact-encode probes and can close a part on stored size.
        #[arg(long, value_name = "BYTES")]
        max_l1_part_bytes: Option<u64>,
        /// Number of per-input reads a compaction keeps in flight at once (the
        /// commit-record GET and catalog load per input). Raise it to hide store
        /// round-trip latency on a many-input bucket; it never changes output
        /// bytes. Default 8 (the compactor default); values below 1 act as 1.
        #[arg(long, value_name = "N")]
        input_read_concurrency: Option<usize>,
        /// Number of buckets to compact CONCURRENTLY. Buckets are independent by
        /// construction (disjoint per-(shard, hour) input sets, separate
        /// content-addressed segments, separate CAS-published records), so the walk
        /// is embarrassingly parallel: N > 1 runs up to N buckets' compactions
        /// at once. Default 1, which is today's fully sequential behavior
        /// byte-for-byte (report line order included). Refused at 0.
        ///
        /// MEMORY: each bucket's merge carries its OWN tracker (ADR-0979), and
        /// each is given a per-bucket SHARE of the merge cursor budget so the
        /// whole N-bucket run still fits one box. ADR-0979's default merge cursor
        /// budget is DEFAULT_MERGE_CURSOR_BUDGET_BYTES = 20 GiB, sized against a
        /// single 30 GB reference box; when you have not configured a budget,
        /// compact-tenant sets each bucket's budget to 20 GiB / N (integer
        /// division, floor). So each concurrent bucket may hold up to ~20 GiB / N
        /// of cursor budget plus its in-progress writer split target
        /// (--l1-part-memory-target-bytes, for a log merge by default the
        /// host's memory less the overhead reserve (2 GiB from 8 GiB of memory
        /// up, below it a quarter of that memory but at least 256 MiB) and less the 20 GiB cursor
        /// budget, / 8 / N, within [256 MiB, 8 GiB]): on a 30 GiB host at N=1 one bucket may hold
        /// ~20 GiB + 1 GiB; at N=4 each of the four holds up to ~5 GiB +
        /// 256 MiB, so the aggregate stays ~20 GiB of cursor budget plus
        /// ~1 GiB of writer targets. Dividing the budget is
        /// what keeps N times the envelope inside one box instead of needing N
        /// boxes. A bucket whose merge no longer fits its 20 GiB / N share fails
        /// closed with the typed MergeCursorBudgetExceeded naming the figure to
        /// raise (a deliberate, visible refusal, not an out-of-memory kill), so
        /// on a small box prefer a lower N over hoping a large merge fits a thin
        /// share. Size N against the box's RAM, not its core count.
        ///
        /// This flag governs how many buckets run at once; --input-read-
        /// concurrency governs the read fan-out WITHIN one bucket. They compose:
        /// total in-flight reads can reach N times --input-read-concurrency.
        #[arg(long, value_name = "N", default_value_t = 1)]
        bucket_concurrency: usize,
        /// The zstd level an RLOG compaction writes its L1 segments at. Higher
        /// levels store smaller segments for more compaction CPU; reads decode
        /// any level the same way. Refused outside 1..=22. Default 9 (the
        /// compactor default).
        #[arg(long, value_name = "LEVEL")]
        compaction_zstd_level: Option<i32>,
        #[command(flatten)]
        tenant_kms: store::TenantKmsArgs,
    },
    /// Run one sweep pass (orphan GC, superseded, unreferenced segments) over a shard.
    Sweep {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long)]
        shard: u32,
        /// Compute the eligible set and report it, but delete nothing.
        #[arg(long)]
        dry_run: bool,
        /// Force exactly one overridden pass through a tripped mass-orphan
        /// circuit breaker (ADR-0048 decision 4). The breaker never
        /// auto-resumes; this is the only way to clear it, and only for this
        /// one invocation.
        #[arg(long)]
        override_orphan_breaker: bool,
    },
    /// Report a bucket's maintenance state (read-only; no --dry-run needed).
    Status {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long)]
        shard: u32,
        #[arg(long)]
        hour: u32,
    },
    /// Audit live on-object format versions for a tenant (metrics, logs and spans), and
    /// classify each recorded format floor; exits nonzero on a contradicted one.
    AuditVersions {
        #[arg(long)]
        tenant: String,
        #[arg(long, default_value_t = 4)]
        shards: u32,
    },
    /// Migrate a (tenant, signal, format family) up to a target format version,
    /// then raise its recorded format floor once a fresh re-audit confirms
    /// nothing below the target survives. Resumable and bounded: re-run to
    /// resume from the durable cursor after a budget stop.
    /// The re-audit already excludes a bucket's pre-rewrite commit records
    /// once that bucket has been rewritten (they are dead, sweepable
    /// leftovers, not stragglers), so a clean run converges and
    /// raises the floor in one invocation with no interleaved `sweep` needed.
    /// A refused raise ("FOUND STRAGGLERS") therefore means genuine
    /// below-target live data (e.g. still-unsealed or newly landed); re-run
    /// migrate once it has settled.
    Migrate {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long, default_value_t = 4)]
        shards: u32,
        /// Target format version to raise the floor to. Defaults to the
        /// signal's current supported on-object version.
        #[arg(long)]
        target_version: Option<u32>,
        /// Lowercase format-family identifier the floor is keyed by. Defaults
        /// to the signal's canonical family (metrics=rseg, logs=rlog,
        /// spans=rspan).
        #[arg(long)]
        family: Option<String>,
        /// Maximum L0 records to migrate this invocation before persisting the
        /// cursor and returning (0 = unlimited; drain the whole walk).
        #[arg(long, default_value_t = 0)]
        budget_records: u64,
        /// Take no bucket claims, for repair work when a claim is in the way.
        /// Each migration publishes a compaction record; against an erasure
        /// rewrite of the same bucket only the pre-publish re-list then fences
        /// that publish, which leaves a short window between the re-list and
        /// the record PUT.
        #[arg(long)]
        no_claim: bool,
        /// Report what is below the target now, from a read-only re-audit, and
        /// write nothing: no migration, no re-encode, no claim, no cursor and
        /// no floor.
        #[arg(long)]
        dry_run: bool,
        /// Re-encode a bucket whose one compaction record holds parts below
        /// the target: write the parts again at the current version and a
        /// version 2 compaction record that supersedes the old one. Every
        /// reader and maintainer in the fleet must already run a build that
        /// reads version 2 compaction records, and the release before the
        /// running one must read them too, so a one-release rollback stays
        /// safe: once one is written there is no rollback past a build that
        /// reads them. Sweep does nothing to the superseded record until the
        /// version 2 record is older than the protection horizon and no HEAD
        /// still names the superseded record's parts; the first sweep pass
        /// after that writes its unnamed-since marker, a pass at least the
        /// pinned-query window later deletes the record and its parts, and the
        /// format floor rises on the first migrate run after that. Off by
        /// default: such a bucket is then reported as reencode_blocked.
        #[arg(long)]
        reencode_compaction_parts: bool,
        #[command(flatten)]
        tenant_kms: store::TenantKmsArgs,
    },
    /// Re-verify the content-addressed chain for a tenant at rest (both
    /// signals): every live data object's content still hashes to the hash16
    /// its key embeds, and every compaction record's referenced inputs still
    /// match. Read-only; no --dry-run (it never writes or deletes).
    VerifyCustody {
        #[arg(long)]
        tenant: String,
        #[arg(long, default_value_t = 4)]
        shards: u32,
        /// Also list noncurrent (prior) versions under the tenant's keys and
        /// report "deleted but recoverable as prior version" as a distinct
        /// anomaly class (ADR-0064 §7, S4-12). The ObjectStoreBackend contract
        /// exposes no versioned listing, so against a real backend this reports
        /// an honest gap rather than an anomaly.
        #[arg(long)]
        versioning_aware: bool,
    },
}

#[derive(Debug, Subcommand)]
enum CatalogCommand {
    List {
        #[arg(long)]
        tenant: String,
        /// How many hours back from now to list commit records for.
        #[arg(long, default_value_t = 1)]
        hours: i64,
        #[arg(long, default_value_t = 4)]
        shards: u32,
    },
    /// One-shot catalog fold for one (tenant, signal).
    ///
    /// A tenant's snapshot is per signal: a logs or spans tenant is never
    /// folded unless `--signal` names it. Folding metrics on a logs-only
    /// tenant seals nothing and publishes an empty metrics HEAD, leaving
    /// every logs query to list and read every commit record.
    Fold {
        #[arg(long)]
        tenant: String,
        #[arg(long, default_value_t = 4)]
        shards: u32,
        /// Which signal's snapshot to fold. Defaults to metrics, so an
        /// existing invocation keeps its meaning.
        #[arg(long, value_enum, default_value = "metrics")]
        signal: SignalArg,
        /// Override the fold's `max_flush_lifetime` (humantime duration, e.g.
        /// `30m`, `0s`; the same grammar and unit as the `maintain
        /// compact-bucket` / `compact-tenant` flag and ravel-server's
        /// `--gc-max-flush-lifetime`). An hour seals only at its end plus this
        /// plus the clock-skew allowance plus the fold safety margin, so a
        /// freshly finished load waits over an hour before its last hours can
        /// be folded; lowering this seals them sooner. The flag asserts that no
        /// writer is still flushing, not that this host's clock is exact: the
        /// clock-skew allowance and the fold safety margin keep their defaults.
        /// UNSAFE under a live writer: a commit record published into a bucket
        /// this fold already sealed is never picked up by a later incremental
        /// fold, which re-lists only hours after the watermark. The default is
        /// the safe 1h; use this only for a tenant known quiescent, such as one
        /// whose bulk load has finished and whose writer process has exited.
        #[arg(long, value_name = "DURATION",
              value_parser = parse_max_flush_lifetime_ns)]
        max_flush_lifetime: Option<i64>,
        /// Print the full `FoldReport` as JSON instead of the human-readable
        /// text report. Either form carries every counter on the report.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        tenant_kms: store::TenantKmsArgs,
    },
    /// Decode and print HEAD and every referenced snapshot part for one
    /// (tenant, signal).
    Inspect {
        #[arg(long)]
        tenant: String,
        /// Which signal's HEAD to decode. Defaults to metrics.
        #[arg(long, value_enum, default_value = "metrics")]
        signal: SignalArg,
    },
    /// Re-list sealed commit records for one (tenant, signal) and diff against
    /// that signal's snapshot; exits nonzero if the snapshot mismatches sealed
    /// history. A missing snapshot is reported (nothing folded yet) and exits
    /// zero.
    Verify {
        #[arg(long)]
        tenant: String,
        /// Which signal's snapshot to verify. Defaults to metrics.
        #[arg(long, value_enum, default_value = "metrics")]
        signal: SignalArg,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(Cli::parse()).await
}

/// Install `scheme` process-wide. A second install of the scheme already in
/// force (a test driving [`run`] more than once) is accepted; a different one
/// is refused.
fn install_tenant_hash_scheme(scheme: ravel_types::TenantHashScheme) -> anyhow::Result<()> {
    match ravel_types::install_tenant_hash_scheme(scheme) {
        Ok(()) => Ok(()),
        Err(rejected) => {
            let probe = TenantId::new("ravel-cli-scheme-probe");
            if rejected.hash(&probe) == probe.hash() {
                Ok(())
            } else {
                anyhow::bail!("tenant-hash scheme was already installed")
            }
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    run_logged(cli, &mut std::io::stderr()).await
}

/// [`run`], writing the `--tenant-kms-config` routing line to `log` rather
/// than stderr.
async fn run_logged(cli: Cli, log: &mut (dyn std::io::Write + Send)) -> anyhow::Result<()> {
    // Before running any subcommand that computes a tenant hash, resolve the
    // bucket's real tenant-hash scheme from `sys/tenancy` and install it
    // process-wide. Without this a hashing command would silently use the
    // v1-unkeyed default, so on a v2-keyed bucket it would address the wrong
    // `t/` prefix (a legal hold written where the server's sweeper never
    // looks, for example). A keyed bucket with no key configured refuses here
    // rather than proceeding under the wrong derivation.
    if command_hashes_tenant(&cli.command) {
        let store = store::build_store(&cli.store)?;
        let configured = tenancy::configured_scheme_from_flags(
            cli.tenancy.tenant_hash_key_file.as_deref(),
            cli.tenancy.tenant_hash_unkeyed,
        )?;
        let scheme = if command_is_write(&cli.command) {
            tenancy::resolve_scheme_for_write(store.as_ref(), configured).await?
        } else {
            tenancy::resolve_scheme(store.as_ref(), configured).await?
        };
        install_tenant_hash_scheme(scheme)?;
    } else if command_is_write(&cli.command) {
        // A writing command that does not hash a tenant (`gc-config set`:
        // `sys/gc` is a bucket-root object) still writes into the same
        // bucket the server must accept at startup, so it clears the same
        // fail-closed gate even though it needs no `TenantHashScheme`
        // installed (issue #1184).
        let store = store::build_store(&cli.store)?;
        let configured = tenancy::configured_scheme_from_flags(
            cli.tenancy.tenant_hash_key_file.as_deref(),
            cli.tenancy.tenant_hash_unkeyed,
        )?;
        tenancy::resolve_scheme_for_write(store.as_ref(), configured).await?;
    }

    match cli.command {
        Command::Segment {
            command: SegmentCommand::Inspect { path },
        } => {
            let bytes = store::read_bytes(&cli.store, &path).await?;
            segment_inspect(&bytes)
        }
        Command::Rlog {
            command: RlogCommand::Inspect { path },
        } => {
            let bytes = store::read_bytes(&cli.store, &path).await?;
            rlog_inspect(&bytes)
        }
        Command::Rlog {
            command:
                RlogCommand::Footprint {
                    tenant,
                    shards,
                    json,
                    objects,
                },
        } => {
            let store = store::build_store(&cli.store)?;
            let report = match tenant {
                Some(tenant) => {
                    let keys = rlog_footprint::tenant_object_keys(
                        std::sync::Arc::clone(&store),
                        cli.store.selection(),
                        &tenant,
                        shards,
                        now_ns()?,
                    )
                    .await?;
                    rlog_footprint::footprint_keys(store.as_ref(), &keys).await?
                }
                None => rlog_footprint::footprint_targets(store.as_ref(), &objects).await?,
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", rlog_footprint::render_text(&report));
            }
            Ok(())
        }
        Command::Rspan {
            command: RspanCommand::Inspect { path },
        } => {
            let bytes = store::read_bytes(&cli.store, &path).await?;
            rspan_inspect(&bytes)
        }
        Command::Commit {
            command: CommitCommand::Decode { key },
        } => {
            let bytes = store::read_bytes(&cli.store, &key).await?;
            commit_decode(&bytes)
        }
        Command::Commit {
            command: CommitCommand::DecodeCompaction { key },
        } => {
            let bytes = store::read_bytes(&cli.store, &key).await?;
            maintain::decode_compaction_record(&bytes)
        }
        Command::Commit {
            command: CommitCommand::DecodeTombstone { key },
        } => {
            let bytes = store::read_bytes(&cli.store, &key).await?;
            maintain::decode_retention_tombstone(&bytes)
        }
        Command::Commit {
            command:
                CommitCommand::Reconstruct {
                    tenant,
                    signal,
                    shard,
                },
        } => {
            ravel_cli::reconstruct::reconstruct(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                signal,
                shard,
            )
            .await
        }
        Command::Catalog {
            command:
                CatalogCommand::List {
                    tenant,
                    hours,
                    shards,
                },
        } => catalog_list(&cli.store, &tenant, hours, shards).await,
        Command::Catalog {
            command:
                CatalogCommand::Fold {
                    tenant,
                    shards,
                    signal,
                    max_flush_lifetime,
                    json,
                    tenant_kms,
                },
        } => catalog::fold(
            store::build_tenant_data_store(&cli.store, &tenant_kms, &tenant, false, now_ns()?, log)
                .await?,
            cli.store.selection(),
            &tenant,
            shards,
            signal,
            max_flush_lifetime,
            now_ns()?,
            json,
        )
        .await
        .map(|_report| ()),
        Command::Inspect {
            command: InspectCommand::Cstat { key },
        } => {
            let bytes = store::read_bytes(&cli.store, &key).await?;
            catalog::inspect_cstat(&bytes)
        }
        Command::Catalog {
            command: CatalogCommand::Inspect { tenant, signal },
        } => {
            catalog::inspect(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                signal,
            )
            .await
        }
        Command::Catalog {
            command: CatalogCommand::Verify { tenant, signal },
        } => {
            catalog::verify(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                signal,
            )
            .await
        }
        Command::Maintain {
            command:
                MaintainCommand::CompactBucket {
                    tenant,
                    signal,
                    shard,
                    hour,
                    dry_run,
                    no_claim,
                    max_flush_lifetime,
                    l1_part_memory_target_bytes,
                    max_l1_part_bytes,
                    compaction_zstd_level,
                    tenant_kms,
                },
        } => {
            maintain::compact_with_part_split_targets(
                store::build_tenant_data_store(
                    &cli.store,
                    &tenant_kms,
                    &tenant,
                    dry_run,
                    now_ns()?,
                    log,
                )
                .await?,
                cli.store.selection(),
                &tenant,
                signal,
                shard,
                hour,
                dry_run,
                max_flush_lifetime,
                l1_part_memory_target_bytes,
                max_l1_part_bytes,
                compaction_zstd_level,
                &maintain::ClaimOptions::for_invocation(no_claim),
            )
            .await
        }
        Command::Maintain {
            command:
                MaintainCommand::CompactTenant {
                    tenant,
                    signal,
                    shards,
                    from_hour,
                    to_hour,
                    dry_run,
                    no_claim,
                    max_flush_lifetime,
                    l1_part_memory_target_bytes,
                    max_l1_part_bytes,
                    input_read_concurrency,
                    bucket_concurrency,
                    compaction_zstd_level,
                    tenant_kms,
                },
        } => maintain::compact_tenant(
            store::build_tenant_data_store(
                &cli.store,
                &tenant_kms,
                &tenant,
                dry_run,
                now_ns()?,
                log,
            )
            .await?,
            cli.store.selection(),
            &tenant,
            signal,
            shards,
            from_hour,
            to_hour,
            dry_run,
            max_flush_lifetime,
            l1_part_memory_target_bytes,
            max_l1_part_bytes,
            input_read_concurrency,
            bucket_concurrency,
            compaction_zstd_level,
            now_ns()?,
            &maintain::ClaimOptions::for_invocation(no_claim),
        )
        .await
        .map(|_| ()),
        Command::Maintain {
            command:
                MaintainCommand::Sweep {
                    tenant,
                    signal,
                    shard,
                    dry_run,
                    override_orphan_breaker,
                },
        } => {
            maintain::sweep(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                signal,
                shard,
                dry_run,
                override_orphan_breaker,
            )
            .await
        }
        Command::Maintain {
            command:
                MaintainCommand::Status {
                    tenant,
                    signal,
                    shard,
                    hour,
                },
        } => {
            maintain::status(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                signal,
                shard,
                hour,
            )
            .await
        }
        Command::Maintain {
            command: MaintainCommand::AuditVersions { tenant, shards },
        } => {
            maintain::audit_versions(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                shards,
            )
            .await
        }
        Command::Maintain {
            command:
                MaintainCommand::VerifyCustody {
                    tenant,
                    shards,
                    versioning_aware,
                },
        } => {
            maintain::verify_custody(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                shards,
                versioning_aware,
            )
            .await
        }
        Command::Maintain {
            command:
                MaintainCommand::Migrate {
                    tenant,
                    signal,
                    shards,
                    target_version,
                    family,
                    budget_records,
                    no_claim,
                    dry_run,
                    reencode_compaction_parts,
                    tenant_kms,
                },
        } => {
            maintain::migrate(
                store::build_tenant_data_store(
                    &cli.store,
                    &tenant_kms,
                    &tenant,
                    dry_run,
                    now_ns()?,
                    log,
                )
                .await?,
                cli.store.selection(),
                &tenant,
                signal,
                shards,
                target_version,
                family,
                budget_records,
                maintain::MigrateSwitches {
                    dry_run,
                    reencode_compaction_parts,
                },
                &maintain::ClaimOptions::for_invocation(no_claim),
            )
            .await
        }
        Command::Store {
            command: StoreCommand::Qualify { list_page_size },
        } => {
            let run_id = uuid::Uuid::new_v4();
            ravel_cli::qualify::qualify_built(
                store::build_store_handle(&cli.store, Some(list_page_size))?,
                cli.store.backend_identity(),
                &run_id.to_string(),
                list_page_size,
            )
            .await
        }
        Command::Store {
            command:
                StoreCommand::VerifyProtection {
                    expected_noncurrent_days,
                    expect_replication,
                },
        } => {
            let expectations = store::ProtectionExpectations {
                expected_noncurrent_days,
                expect_replication,
            };
            let outcome = match store::build_store_handle(&cli.store, None) {
                Ok(built) => store::verify_protection(&built, expectations).await,
                Err(err) => store::verify_protection_unreachable(&err, expectations),
            };
            if let Err(err) =
                store::write_verify_protection(&mut std::io::stdout().lock(), &outcome)
            {
                eprintln!("error: could not write the verify-protection report: {err}");
                std::process::exit(store::VERIFY_PROTECTION_UNKNOWN);
            }
            if outcome.exit_code != store::VERIFY_PROTECTION_PASS {
                std::process::exit(outcome.exit_code);
            }
            Ok(())
        }
        Command::Hold {
            command:
                HoldCommand::Set {
                    tenant,
                    scope,
                    signal,
                    shard,
                    reason,
                },
        } => {
            hold::set(
                store::build_store(&cli.store)?,
                &tenant,
                scope,
                signal,
                shard,
                &reason,
            )
            .await
        }
        Command::Hold {
            command:
                HoldCommand::Clear {
                    tenant,
                    scope,
                    signal,
                    shard,
                },
        } => {
            hold::clear(
                store::build_store(&cli.store)?,
                &tenant,
                scope,
                signal,
                shard,
            )
            .await
        }
        Command::Hold {
            command: HoldCommand::List { tenant },
        } => hold::list(store::build_store(&cli.store)?, &tenant).await,
        Command::Erase {
            command:
                EraseCommand::Submit {
                    tenant,
                    signal,
                    matchers,
                    window_start_ns,
                    window_end_ns,
                    reason,
                    request_id,
                },
        } => {
            let matchers = matchers
                .iter()
                .map(|m| ravel_cli::erase::parse_matcher(m))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let request_id = match request_id {
                Some(s) => uuid::Uuid::parse_str(&s)
                    .map_err(|_| anyhow::anyhow!("--request-id {s:?} is not a valid UUID"))?,
                None => uuid::Uuid::new_v4(),
            };
            ravel_cli::erase::submit(
                store::build_store(&cli.store)?,
                &tenant,
                signal,
                matchers,
                window_start_ns,
                window_end_ns,
                reason,
                request_id,
                now_ns()?,
            )
            .await
            .map(|_| ())
        }
        Command::Erase {
            command:
                EraseCommand::Status {
                    tenant,
                    signal,
                    request_id,
                },
        } => {
            let request_id = match request_id {
                Some(s) => Some(
                    uuid::Uuid::parse_str(&s)
                        .map_err(|_| anyhow::anyhow!("--request-id {s:?} is not a valid UUID"))?,
                ),
                None => None,
            };
            ravel_cli::erase::status(store::build_store(&cli.store)?, &tenant, signal, request_id)
                .await
        }
        Command::Idem {
            command: IdemCommand::Inspect { key },
        } => {
            let report = idem::inspect(store::build_store(&cli.store)?, &key).await?;
            println!("{report}");
            Ok(())
        }
        Command::Tenancy {
            command: TenancyCommand::Show {
                tenant_hash_key_file,
            },
        } => {
            let report = tenancy::show(
                store::build_store(&cli.store)?,
                tenant_hash_key_file.as_deref(),
            )
            .await?;
            print!("{report}");
            Ok(())
        }
        Command::Tenancy {
            command: TenancyCommand::Resolve { tenant },
        } => {
            let configured = tenancy::configured_scheme_from_flags(
                cli.tenancy.tenant_hash_key_file.as_deref(),
                cli.tenancy.tenant_hash_unkeyed,
            )?;
            let scheme =
                tenancy::resolve_scheme(store::build_store(&cli.store)?.as_ref(), configured)
                    .await?;
            println!("{}", tenancy::resolve(&scheme, &tenant));
            Ok(())
        }
        Command::Provision {
            command:
                ProvisionCommand::Adopt {
                    tenant,
                    shards,
                    signal,
                },
        } => {
            ravel_cli::provision::adopt(
                store::build_store(&cli.store)?,
                &tenant,
                shards,
                signal,
                now_ns()?,
            )
            .await
        }
        Command::Provision {
            command:
                ProvisionCommand::Reshard {
                    tenant,
                    signal,
                    shard_count,
                    lead_hours,
                },
        } => {
            ravel_cli::provision::reshard(
                store::build_store(&cli.store)?,
                &tenant,
                signal,
                shard_count,
                lead_hours,
                now_ns()?,
            )
            .await
        }
        Command::TypedAttrColumn {
            command: TypedAttrColumnCommand::Show { tenant },
        } => ravel_cli::typed_attr_column::show(store::build_store(&cli.store)?, &tenant).await,
        Command::ClusteringKey {
            command: ClusteringKeyCommand::Show { tenant },
        } => {
            ravel_cli::storage_layout::clustering_key_show(store::build_store(&cli.store)?, &tenant)
                .await
        }
        Command::ClusteringKey {
            command:
                ClusteringKeyCommand::Set {
                    tenant,
                    columns,
                    bucket_width,
                    rollout,
                },
        } => {
            ravel_cli::storage_layout::clustering_key_set(
                store::build_store(&cli.store)?,
                &tenant,
                columns,
                bucket_width,
                rollout.write(),
                now_ns()?,
            )
            .await
        }
        Command::ClusteringKey {
            command: ClusteringKeyCommand::Clear { tenant, rollout },
        } => {
            ravel_cli::storage_layout::clustering_key_clear(
                store::build_store(&cli.store)?,
                &tenant,
                rollout.write(),
                now_ns()?,
            )
            .await
        }
        Command::BloomScope {
            command: BloomScopeCommand::Show { tenant },
        } => {
            ravel_cli::storage_layout::bloom_scope_show(store::build_store(&cli.store)?, &tenant)
                .await
        }
        Command::BloomScope {
            command:
                BloomScopeCommand::Set {
                    tenant,
                    scope,
                    rollout,
                },
        } => {
            ravel_cli::storage_layout::bloom_scope_set(
                store::build_store(&cli.store)?,
                &tenant,
                scope,
                rollout.write(),
                now_ns()?,
            )
            .await
        }
        Command::TypedAttrColumn {
            command:
                TypedAttrColumnCommand::Set {
                    tenant,
                    columns,
                    from_mapping,
                },
        } => match from_mapping {
            Some(mapping_path) => {
                ravel_cli::typed_attr_column::set_from_mapping(
                    store::build_store(&cli.store)?,
                    &tenant,
                    &mapping_path,
                    now_ns()?,
                )
                .await
            }
            None => {
                ravel_cli::typed_attr_column::set(
                    store::build_store(&cli.store)?,
                    &tenant,
                    &columns,
                    now_ns()?,
                )
                .await
            }
        },
        Command::GcConfig {
            command: GcConfigCommand::Show {},
        } => ravel_cli::gc_config::show(store::build_store(&cli.store)?).await,
        Command::GcConfig {
            command:
                GcConfigCommand::Set {
                    protection_horizon,
                    grace,
                    max_query_duration,
                    max_flush_lifetime,
                    clock_skew_allowance,
                    head_cache_ttl,
                },
        } => {
            ravel_cli::gc_config::set(
                store::build_store(&cli.store)?,
                &protection_horizon,
                &grace,
                &max_query_duration,
                &max_flush_lifetime,
                clock_skew_allowance.as_deref(),
                head_cache_ttl.as_deref(),
                now_ns()?,
            )
            .await
        }
        Command::Tenant {
            command:
                TenantCommand::Token {
                    command:
                        TenantTokenCommand::Upsert {
                            deployment_key_file,
                            token,
                            tenant,
                            managed_by,
                        },
                },
        } => {
            tenant_token::upsert(
                store::build_store(&cli.store)?,
                &deployment_key_file,
                token.as_bytes(),
                &tenant,
                &managed_by,
                now_ns()?,
            )
            .await
        }
        Command::Tenant {
            command:
                TenantCommand::Token {
                    command:
                        TenantTokenCommand::Revoke {
                            deployment_key_file,
                            tenant,
                        },
                },
        } => {
            tenant_token::revoke(
                store::build_store(&cli.store)?,
                &deployment_key_file,
                &tenant,
                now_ns()?,
            )
            .await
        }
        Command::Tenant {
            command:
                TenantCommand::Token {
                    command:
                        TenantTokenCommand::List {
                            deployment_key_file,
                        },
                },
        } => {
            let report =
                tenant_token::list(store::build_store(&cli.store)?, &deployment_key_file).await?;
            print!("{report}");
            Ok(())
        }
        Command::Tenant {
            command:
                TenantCommand::ParquetGrant {
                    command:
                        TenantParquetGrantCommand::Add {
                            tenant,
                            location,
                            profile,
                        },
                },
        } => {
            ravel_cli::parquet_grant::add(
                store::build_store(&cli.store)?,
                cli.parquet_profiles.as_deref(),
                &tenant,
                &location,
                &profile,
                "ravel-cli",
                now_ns()?,
            )
            .await
        }
        Command::Tenant {
            command:
                TenantCommand::ParquetGrant {
                    command: TenantParquetGrantCommand::Remove { tenant, location },
                },
        } => {
            ravel_cli::parquet_grant::remove(store::build_store(&cli.store)?, &tenant, &location)
                .await
        }
        Command::Tenant {
            command:
                TenantCommand::ParquetGrant {
                    command: TenantParquetGrantCommand::Ls { tenant },
                },
        } => ravel_cli::parquet_grant::ls(store::build_store(&cli.store)?, &tenant).await,
        Command::Parquet {
            command: ParquetCommand::Ls { tenant, table },
        } => {
            ravel_cli::parquet::ls(store::build_store(&cli.store)?, &tenant, table.as_deref()).await
        }
        Command::Parquet {
            command: ParquetCommand::Sweep { tenant, grace },
        } => {
            ravel_cli::parquet::sweep(store::build_store(&cli.store)?, &tenant, &grace, now_ns()?)
                .await
        }
        Command::Parquet {
            command:
                ParquetCommand::Repair {
                    tenant,
                    table,
                    stray,
                    delete,
                    delete_version,
                    include_reserved_names,
                },
        } => {
            use ravel_cli::parquet::RepairAction;
            let store = store::build_store(&cli.store)?;
            // clap makes `--stray` and `--table` exclusive and requires one.
            match table.filter(|_| !stray) {
                None => {
                    ravel_cli::parquet::repair_stray(store, &tenant, delete, include_reserved_names)
                        .await
                }
                Some(table) => {
                    let action = match (delete, delete_version) {
                        (_, Some(version)) => RepairAction::DeleteVersion(version),
                        (true, None) => RepairAction::DeleteFlagged,
                        (false, None) => RepairAction::List,
                    };
                    ravel_cli::parquet::repair(store, &tenant, &table, action).await
                }
            }
        }
        Command::Cache {
            command: CacheCommand::ReclaimLegacy { cache_dir, apply },
        } => cache_reclaim_legacy(&cache_dir, apply),
        Command::Load {
            parquet,
            tenant,
            mapping,
            signal,
            shards,
            batch_rows,
            skip_rows,
            read_cursors,
            pipeline_depth,
            max_inflight_flushes,
            decode_queue_batches,
            target_bytes,
            max_flush_delay,
            zstd_level,
        } => {
            let profile = ravel_cli::cli_profiling::ProfileSession::from_env("ravel-cli-load");
            let result = ravel_cli::load::run(
                store::build_store(&cli.store)?,
                &parquet,
                &tenant,
                &mapping,
                signal,
                shards,
                batch_rows,
                skip_rows,
                read_cursors,
                pipeline_depth,
                max_inflight_flushes,
                decode_queue_batches,
                target_bytes,
                max_flush_delay,
                zstd_level,
                now_ns()?,
            )
            .await;
            profile.finish();
            result
        }
        Command::Export {
            signal,
            tenant,
            start,
            end,
            parquet,
            mapping,
            shards,
            max_ingest_lag,
        } => {
            ravel_cli::export::run(
                store::build_store(&cli.store)?,
                cli.store.selection(),
                &tenant,
                signal,
                start,
                end,
                &mapping,
                &parquet,
                shards,
                max_ingest_lag,
                now_ns()?,
            )
            .await
        }
    }
}

/// Human-readable name for a known section kind, for the `sections:`
/// listing. Falls back to "UNKNOWN" for any kind not in the frozen set above,
/// matching how readers must skip unknown kinds rather than reject them.
fn section_kind_name(kind: u32) -> &'static str {
    match kind {
        SECTION_KIND_LABEL_DICT => "LABEL_DICT",
        SECTION_KIND_SERIES_TABLE => "SERIES_TABLE",
        SECTION_KIND_TS_PAGES => "TS_PAGES",
        SECTION_KIND_VAL_PAGES => "VAL_PAGES",
        SECTION_KIND_SERIES_IDS => "SERIES_IDS",
        SECTION_KIND_SERIES_META => "SERIES_META",
        SECTION_KIND_HIST_PAGES => "HIST_PAGES",
        SECTION_KIND_SERIES_IDX => "SERIES_IDX",
        SECTION_KIND_SERIES_META_CHUNKS => "SERIES_META_CHUNKS",
        SECTION_KIND_EXEMPLARS => "EXEMPLARS",
        _ => "UNKNOWN",
    }
}

/// Human-readable name for a `ravel_segment::ValueKind`, matching the wire
/// names from SERIES_META column 10 (`0 = VAL_SCALAR, 1 = HIST_SPANS`), not
/// `ValueKind`'s Rust variant names.
fn value_kind_name(kind: ravel_segment::ValueKind) -> &'static str {
    match kind {
        ravel_segment::ValueKind::Scalar => "VAL_SCALAR",
        ravel_segment::ValueKind::Histogram => "HIST_SPANS",
    }
}

/// Human-readable name for a `ravel_segment::ResetHint`, matching
/// Prometheus's four reset-hint states.
fn reset_hint_name(hint: ravel_segment::ResetHint) -> &'static str {
    match hint {
        ravel_segment::ResetHint::Unknown => "UNKNOWN",
        ravel_segment::ResetHint::Yes => "YES",
        ravel_segment::ResetHint::No => "NO",
        ravel_segment::ResetHint::Gauge => "GAUGE",
    }
}

fn format_spans(spans: &[ravel_segment::HistogramSpan]) -> String {
    spans
        .iter()
        .map(|s| format!("({}, {})", s.offset, s.length))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_f64_list(values: &[f64]) -> String {
    values
        .iter()
        .map(f64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn format_u64_list(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Absolute `[start, start+len)` slice of `bytes`, for the ranges
/// `plan_ranges_v3` computes (already section-bounds-checked; this only
/// guards the final slice against the object's own total size).
fn absolute_range(bytes: &[u8], range: (u64, u64)) -> anyhow::Result<&[u8]> {
    let start = usize::try_from(range.0)?;
    let end = start
        .checked_add(usize::try_from(range.1)?)
        .ok_or_else(|| anyhow::anyhow!("byte range overflows"))?;
    bytes
        .get(start..end)
        .ok_or_else(|| anyhow::anyhow!("byte range is out of bounds"))
}

/// Prints one decoded histogram record:
/// scale, zero bucket, count, sum, reset_hint, then per-side spans and
/// bucket counts. A histogram series can hold more than one sample
/// (`sample_count` in its SERIES_META row), so this is called once per
/// decoded `HistogramSample`, indexed within its series.
fn print_histogram_sample(index: usize, sample: &ravel_segment::HistogramSample) {
    let value = &sample.value;
    let custom_values = match &value.custom_values {
        Some(bounds) => format!(" custom_values=[{}]", format_f64_list(bounds)),
        None => String::new(),
    };
    let sum = value
        .sum
        .map(|s| s.to_string())
        .unwrap_or_else(|| "none".to_string());
    println!(
        "    hist[{index}]: ts_ns={} scale={} zero_threshold={} sum={} reset_hint={}{}",
        sample.ts_ns,
        value.scale,
        value.zero_threshold,
        sum,
        reset_hint_name(value.reset_hint),
        custom_values
    );
    match &value.counts {
        ravel_segment::HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => {
            println!("      count_kind=INT zero_count={zero_count} count={count}");
            println!(
                "      positive: spans=[{}] counts=[{}]",
                format_spans(&value.positive_spans),
                format_u64_list(positive)
            );
            println!(
                "      negative: spans=[{}] counts=[{}]",
                format_spans(&value.negative_spans),
                format_u64_list(negative)
            );
        }
        ravel_segment::HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => {
            println!("      count_kind=FLOAT zero_count={zero_count} count={count}");
            println!(
                "      positive: spans=[{}] counts=[{}]",
                format_spans(&value.positive_spans),
                format_f64_list(positive)
            );
            println!(
                "      negative: spans=[{}] counts=[{}]",
                format_spans(&value.negative_spans),
                format_f64_list(negative)
            );
        }
    }
}

fn segment_inspect(bytes: &[u8]) -> anyhow::Result<()> {
    let limits = ravel_segment::ReaderLimits::default();
    let location = ravel_segment::open_from_full(bytes, limits)
        .map_err(|err| anyhow::anyhow!("failed to parse segment: {err}"))?;
    let footer = &location.footer;

    println!("total_size: {}", location.total_size);
    println!("trailer_offset: {}", location.trailer_offset);
    println!("version: {}", location.version);
    println!("footer_offset: {}", location.footer_offset);
    println!("tenant_hash: {}", hex::encode(&footer.tenant_hash));
    println!("shard: {}", footer.shard);
    println!("writer_id: {}", footer.writer_id);
    println!("writer_epoch: {}", footer.writer_epoch);
    println!("writer_seq: {}", footer.writer_seq);
    println!("min_event_ts_ns: {}", footer.min_event_ts_ns);
    println!("max_event_ts_ns: {}", footer.max_event_ts_ns);
    println!("min_ingest_ts_ns: {}", footer.min_ingest_ts_ns);
    println!("max_ingest_ts_ns: {}", footer.max_ingest_ts_ns);
    println!("sample_count: {}", footer.sample_count);
    println!("series_count (footer): {}", footer.series_count);
    println!("base_created_unix_ns: {}", footer.base_created_unix_ns);
    println!("level: {}", footer.level);
    println!("input_set_hash: {}", hex::encode(&footer.input_set_hash));
    println!("part_index: {}", footer.part_index);
    println!("sections:");
    for section in &footer.sections {
        println!(
            "  kind={} name={} offset={} len={} uncompressed_len={} comp={:?}",
            section.kind,
            section_kind_name(section.kind),
            section.offset,
            section.len,
            section.uncompressed_len,
            section.comp
        );
    }

    segment_inspect_v6(bytes, footer, limits)
}

/// v6 catalog decode and print (docs/segment-format.md). ADR-0047 leaves v6
/// the only version; the run-major catalog is decoded over the whole object
/// (folding the chunked SERIES_META, or the whole SERIES_META below the
/// sparse threshold), and each series prints its per-run provenance and page
/// ranges. `schema_count` is derived from the decoded label sets (distinct
/// name-only tuples, first-appearance order), the same "(derived)" caveat the
/// pre-v5 inspector carried. Histogram runs decode their HIST/TS pages and
/// print every record's full field detail. EXEMPLARS (ADR-0047) is optional
/// and printed last, since it is object-wide rather than per series.
fn segment_inspect_v6(
    bytes: &[u8],
    footer: &Footer,
    limits: ravel_segment::ReaderLimits,
) -> anyhow::Result<()> {
    let entries = ravel_segment::decode_catalog_v5(footer, bytes, limits)
        .map_err(|err| anyhow::anyhow!("failed to decode series catalog: {err}"))?;

    let mut schemas: Vec<Vec<String>> = Vec::new();
    for entry in &entries {
        let names: Vec<String> = entry.entry.labels.iter().map(|l| l.name.clone()).collect();
        if !schemas.contains(&names) {
            schemas.push(names);
        }
    }
    println!("schema_count (derived): {}", schemas.len());
    for (i, schema) in schemas.iter().enumerate() {
        println!("  schema[{i}]: {}", schema.join(","));
    }

    println!("series_count (decoded): {}", entries.len());

    let selected: Vec<&ravel_segment::SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(footer, &selected)
        .map_err(|err| anyhow::anyhow!("failed to plan page ranges: {err}"))?;

    println!("series:");
    for entry in &entries {
        let labels_str = entry
            .entry
            .labels
            .iter()
            .map(|l| format!("{}={}", l.name, l.value))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "  series_id={} labels={} sample_count={} min_ts_ns={} max_ts_ns={} \
             value_kind={} run_count={}",
            hex::encode(entry.entry.series_id.0),
            labels_str,
            entry.entry.sample_count,
            entry.entry.min_ts_ns,
            entry.entry.max_ts_ns,
            value_kind_name(entry.entry.value_kind),
            entry.runs.len(),
        );
        for (run_index, run) in entry.runs.iter().enumerate() {
            let range = ranges
                .iter()
                .find(|p| p.series_id == entry.entry.series_id && p.run_index == run_index)
                .ok_or_else(|| anyhow::anyhow!("no planned range for run {run_index}"))?;
            match entry.entry.value_kind {
                ravel_segment::ValueKind::Scalar => {
                    println!(
                        "    run[{run_index}] created_unix_ns={} writer_epoch={} \
                         writer_seq={} sample_count={} ts_range=[{}, {}) val_range=[{}, {})",
                        run.created_unix_ns,
                        run.writer_epoch,
                        run.writer_seq,
                        run.sample_count,
                        range.ts_range.0,
                        range.ts_range.0.saturating_add(range.ts_range.1),
                        range.val_range.0,
                        range.val_range.0.saturating_add(range.val_range.1),
                    );
                }
                ravel_segment::ValueKind::Histogram => {
                    println!(
                        "    run[{run_index}] created_unix_ns={} writer_epoch={} \
                         writer_seq={} sample_count={} ts_range=[{}, {}) hist_range=[{}, {})",
                        run.created_unix_ns,
                        run.writer_epoch,
                        run.writer_seq,
                        run.sample_count,
                        range.ts_range.0,
                        range.ts_range.0.saturating_add(range.ts_range.1),
                        range.hist_range.0,
                        range.hist_range.0.saturating_add(range.hist_range.1),
                    );
                    let ts_bytes = absolute_range(bytes, range.ts_range)?;
                    let hist_bytes = absolute_range(bytes, range.hist_range)?;
                    let samples = ravel_segment::decode_run_histogram_pages(
                        &entry.entry.series_id,
                        run,
                        ts_bytes,
                        hist_bytes,
                        limits,
                    )
                    .map_err(|err| anyhow::anyhow!("failed to decode histogram pages: {err}"))?;
                    for (i, sample) in samples.iter().enumerate() {
                        print_histogram_sample(i, sample);
                    }
                }
            }
        }
    }

    print_exemplars(bytes, footer, limits)?;

    Ok(())
}

/// EXEMPLARS is optional (ADR-0047): absent means no exemplars were attached
/// to this object, not an empty section, so `decode_exemplars_section`
/// returns an empty list either way and there is nothing else to
/// distinguish here.
fn print_exemplars(
    bytes: &[u8],
    footer: &Footer,
    limits: ravel_segment::ReaderLimits,
) -> anyhow::Result<()> {
    let Some(exemplars_section) = footer
        .sections
        .iter()
        .find(|s| s.kind == SECTION_KIND_EXEMPLARS)
    else {
        println!("exemplar_count: 0");
        return Ok(());
    };
    let label_dict_section = footer
        .sections
        .iter()
        .find(|s| s.kind == SECTION_KIND_LABEL_DICT)
        .ok_or_else(|| anyhow::anyhow!("EXEMPLARS present without LABEL_DICT"))?;
    let label_dict_bytes =
        absolute_range(bytes, (label_dict_section.offset, label_dict_section.len))?;
    let exemplars_bytes = absolute_range(bytes, (exemplars_section.offset, exemplars_section.len))?;
    let exemplars =
        ravel_segment::decode_exemplars_section(footer, label_dict_bytes, exemplars_bytes, limits)
            .map_err(|err| anyhow::anyhow!("failed to decode exemplars: {err}"))?;

    println!("exemplar_count: {}", exemplars.len());
    for (i, ex) in exemplars.iter().enumerate() {
        let attrs = ex
            .attrs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "  exemplar[{i}]: series_index={} ts_ns={} value={} trace_id={} span_id={} attrs={}",
            ex.series_index,
            ex.ts_ns,
            ex.value,
            hex::encode(ex.trace_id),
            hex::encode(ex.span_id),
            attrs
        );
    }
    Ok(())
}

/// Human-readable name for a known RLOG section kind (docs/log-segment-format.md).
fn rlog_section_kind_name(kind: u32) -> &'static str {
    match kind {
        kind::STREAM_DIR => "STREAM_DIR",
        kind::FIELD_DIR => "FIELD_DIR",
        kind::BLOCKS => "BLOCKS",
        kind::SKIP_IDX => "SKIP_IDX",
        kind::BLOOM => "BLOOM",
        kind::POSTINGS => "POSTINGS",
        kind::PAGE_DIR => "PAGE_DIR",
        _ => "UNKNOWN",
    }
}

/// Human-readable name for a section `comp` tag (0=none, 2=zstd).
fn rlog_comp_name(comp: u8) -> &'static str {
    match comp {
        COMP_NONE => "none",
        COMP_ZSTD => "zstd",
        _ => "unknown",
    }
}

/// Human-readable name for a field/stat [`FieldType`] (docs/log-segment-format.md
/// FIELD_DIR type byte: 1=str 2=i64 3=f64 4=bool 5=bytes).
fn rlog_field_type_name(ty: FieldType) -> &'static str {
    match ty {
        FieldType::Str => "str",
        FieldType::I64 => "i64",
        FieldType::F64 => "f64",
        FieldType::Bool => "bool",
        FieldType::Bytes => "bytes",
    }
}

/// Prints the numeric stats attached to a skip-index entry, one per line.
///
/// Under RLOG v3 (ADR-0095) `min_bits`/`max_bits` bound the value each row
/// *resolves* for the column's attribute name -- the row's resource and scope
/// layers overridden by its own attributes -- and `null_count` counts every row
/// whose resolved value for that name is of another type, or which resolves it
/// to nothing. So a stat's bounds can legitimately exclude a value that is
/// sitting in the same column's value page, can cover a value that is in no
/// value page at all (a name the rows resolve off their stream), and can appear
/// on a block that has no page for the column whatsoever. `null_count` can
/// likewise exceed the FIELD_DIR `null_count` printed below, which still counts
/// raw column presence.
fn rlog_print_stats(stats: &[NumStat]) {
    for st in stats {
        println!(
            "    stat column_id={} type={} min_bits={} max_bits={} null_count={} has_nan={} \
             resolved_min={} resolved_max={}",
            st.column_id,
            rlog_field_type_name(st.ty),
            st.min_bits,
            st.max_bits,
            st.null_count,
            st.has_nan,
            rlog_stat_value(st.ty, st.min_bits),
            rlog_stat_value(st.ty, st.max_bits),
        );
    }
}

/// Decodes a NumStat `min_bits`/`max_bits` bit pattern to its typed value for
/// display. Under RLOG v3 (ADR-0095) this is the resolved merged-view bound an
/// operator reasons about when a range prune keeps or drops a block, so the
/// inspector prints it decoded next to the raw bits rather than leaving an
/// operator to reinterpret an `i64`-as-`u64` or an `f64::to_bits` pattern by
/// hand. A NumStat only ever carries a numeric type; the string arms are
/// defensive and never reached.
fn rlog_stat_value(ty: FieldType, bits: u64) -> String {
    match ty {
        FieldType::I64 => (bits as i64).to_string(),
        FieldType::F64 => f64::from_bits(bits).to_string(),
        FieldType::Bool => (bits != 0).to_string(),
        FieldType::Str | FieldType::Bytes => format!("{bits}"),
    }
}

/// Inspects a whole RLOG object (docs/log-segment-format.md): footer identity
/// and summary, the sort descriptor and clustering generation (ADR-2135), the
/// section table, the level-0 skip index (one line per block plus its numeric
/// column stats), the stream directory, the field directory, and the columns
/// BLOOM covers. Every decode is the reader's own path, so a corrupt SKIP_IDX or a
/// section crc mismatch surfaces as a typed error with a non-zero exit, never a
/// panic. BLOOM also goes through `read_section`, so its whole-section crc is
/// verified and a damaged BLOOM is refused here, where a scan never consults
/// that crc and at worst prunes less: an inspector reports damage that a query
/// is allowed to survive.
fn rlog_inspect(bytes: &[u8]) -> anyhow::Result<()> {
    let footer = footer::open(bytes)
        .map_err(|err| anyhow::anyhow!("failed to parse rlog segment: {err}"))?;

    println!("total_size: {}", bytes.len());
    let version = footer::trailer_version(bytes)
        .map_err(|err| anyhow::anyhow!("failed to parse rlog segment: {err}"))?;
    println!("version: {version}");
    println!("signal: {}", footer::SIGNAL_LOGS);
    println!("tenant_hash: {}", hex::encode(footer.tenant_hash));
    println!("shard: {}", footer.shard);
    println!("writer_id: {}", hex::encode(footer.writer_id));
    println!("writer_epoch: {}", footer.writer_epoch);
    println!("writer_seq: {}", footer.writer_seq);
    println!("min_ts_ns: {}", footer.min_ts_ns);
    println!("max_ts_ns: {}", footer.max_ts_ns);
    println!("min_observed_ts_ns: {}", footer.min_observed_ts_ns);
    println!("max_observed_ts_ns: {}", footer.max_observed_ts_ns);
    println!("record_count: {}", footer.record_count);
    println!("block_count: {}", footer.block_count);
    println!("stream_count: {}", footer.stream_count);
    println!("level: {}", footer.level);
    println!("input_set_hash: {}", hex::encode(&footer.input_set_hash));
    println!("part_index: {}", footer.part_index);
    match &footer.sort_descriptor {
        None => println!("sort_descriptor: none"),
        Some(descriptor) => {
            println!(
                "sort_descriptor: bucket_width={} key_columns={}",
                rlog_sort_bucket_width_name(descriptor.bucket_width),
                descriptor.key_columns.len()
            );
            for (i, key) in descriptor.key_columns.iter().enumerate() {
                println!(
                    "  key[{i}] name={} type={}",
                    key.name,
                    rlog_sort_key_type_name(key.ty)
                );
            }
        }
    }
    println!("clustering_generation: {}", footer.clustering_generation);
    println!("sections:");
    for section in &footer.sections {
        println!(
            "  kind={} name={} offset={} len={} comp={} uncompressed_len={}",
            section.kind,
            rlog_section_kind_name(section.kind),
            section.offset,
            section.len,
            rlog_comp_name(section.comp),
            section.uncomp_len,
        );
    }

    // Whole-read sections are reconstructed through ravel-logseg's own
    // `read_section` (the reader's crc-verify-and-decompress path), so the
    // inspector applies the exact `Corrupted` discipline the reader does. The
    // default config's per-section cap matches the open-time validation cap.
    let cfg = RlogConfig::default();
    let section = |k: u32| {
        footer
            .section(k)
            .ok_or_else(|| anyhow::anyhow!("missing section kind {k}"))
    };

    // Skip index, level 0: the block framing and per-block stats.
    let skip_raw = read_section(bytes, section(kind::SKIP_IDX)?, &cfg)
        .map_err(|err| anyhow::anyhow!("failed to read skip index section: {err}"))?;
    let skip = SkipIndex::decode(&skip_raw, RLOG_MAX_BLOCKS)
        .map_err(|err| anyhow::anyhow!("failed to decode skip index: {err}"))?;
    println!("skip_index level 0 ({} block(s)):", skip.l0.len());
    for (i, entry) in skip.l0.iter().enumerate() {
        println!(
            "  block[{i}] offset={} len={} crc32c={:08x} record_count={} \
             ts_range=[{}, {}] stream_ref_range=[{}, {}]",
            entry.block_offset,
            entry.block_len,
            entry.block_crc32c,
            entry.record_count,
            entry.min_ts,
            entry.max_ts,
            entry.min_stream_ref,
            entry.max_stream_ref,
        );
        rlog_print_stats(&entry.stats);
    }

    // Stream directory: stream_id -> ordinal stream_ref and block range.
    let stream_raw = read_section(bytes, section(kind::STREAM_DIR)?, &cfg)
        .map_err(|err| anyhow::anyhow!("failed to read stream directory section: {err}"))?;
    let stream_dir = StreamDir::decode(&stream_raw, RLOG_MAX_STREAMS)
        .map_err(|err| anyhow::anyhow!("failed to decode stream directory: {err}"))?;
    println!("stream_dir ({} entry(ies)):", stream_dir.len());
    for (stream_ref, entry) in stream_dir.entries().iter().enumerate() {
        println!(
            "  stream_ref={} stream_id={} blob_len={} blocks=[{}, {}]",
            stream_ref,
            hex::encode(entry.stream_id.0),
            entry.blob.len(),
            entry.first_blk,
            entry.last_blk,
        );
    }

    // Field directory: dynamic attribute columns.
    let field_raw = read_section(bytes, section(kind::FIELD_DIR)?, &cfg)
        .map_err(|err| anyhow::anyhow!("failed to read field directory section: {err}"))?;
    let field_dir = FieldDir::decode(&field_raw, RLOG_MAX_FIELDS)
        .map_err(|err| anyhow::anyhow!("failed to decode field directory: {err}"))?;
    println!("field_dir ({} entry(ies)):", field_dir.len());
    for entry in field_dir.entries() {
        println!(
            "  column_id={} name={} type={} present_blocks={} null_count={}",
            entry.column_id,
            entry.name,
            rlog_field_type_name(entry.ty),
            entry.present_blocks,
            entry.null_count,
        );
    }

    // BLOOM coverage: the columns the filters cover, named through FIELD_DIR.
    let bloom_raw = read_section(bytes, section(kind::BLOOM)?, &cfg)
        .map_err(|err| anyhow::anyhow!("failed to read bloom section: {err}"))?;
    let bloom = RlogBloomSection::parse(&bloom_raw, &field_dir)
        .map_err(|err| anyhow::anyhow!("failed to decode bloom section: {err}"))?;
    println!("bloom_coverage ({} column(s)):", bloom.covered().len());
    for &column_id in bloom.covered() {
        let (name, column_kind) = rlog_footprint::column_name(column_id, &field_dir);
        println!("  column_id={column_id} name={name} kind={column_kind}");
    }

    Ok(())
}

fn rlog_sort_bucket_width_name(width: SortBucketWidth) -> &'static str {
    match width {
        SortBucketWidth::OneHour => "1h",
        SortBucketWidth::SixHours => "6h",
        SortBucketWidth::OneDay => "1d",
    }
}

fn rlog_sort_key_type_name(ty: SortKeyType) -> &'static str {
    match ty {
        SortKeyType::Str => "str",
        SortKeyType::I64 => "i64",
        SortKeyType::Bool => "bool",
        SortKeyType::Bytes => "bytes",
    }
}

/// Readable flag names for an RSPAN v2 block `status_mask`
/// (docs/span-segment-format.md "SKIP_IDX"). The bit values come from
/// `ravel_rspan::skip_index`'s `STATUS_BIT_*` constants rather than being
/// respelled here, so this rendering cannot drift from the stored flag
/// definitions; the names mirror the `StatusCode` variants those bits
/// summarize (Unset/Ok/Error). A set bit the table does not name is printed
/// as `bit<n>` (its zero-based position), never dropped: an inspector that
/// silently hid an unrecognized bit would make "not understood"
/// indistinguishable from "not set", which is worse than showing a number.
/// An all-zero mask on a non-empty block would be a writer bug, so it renders
/// as `none` rather than an empty string.
fn rspan_status_mask_names(mask: u8) -> String {
    use ravel_rspan::skip_index::{STATUS_BIT_ERROR, STATUS_BIT_OK, STATUS_BIT_UNSET};

    if mask == 0 {
        return "none".to_string();
    }
    let mut names = Vec::new();
    let mut named = 0u8;
    for (bit, name) in [
        (STATUS_BIT_UNSET, "unset"),
        (STATUS_BIT_OK, "ok"),
        (STATUS_BIT_ERROR, "error"),
    ] {
        if mask & bit != 0 {
            names.push(name.to_string());
            named |= bit;
        }
    }
    // Whatever bits remain are ones the flag table does not name; surface each
    // by position instead of dropping it.
    let mut unknown = mask & !named;
    while unknown != 0 {
        let bit = unknown.trailing_zeros();
        names.push(format!("bit{bit}"));
        unknown &= unknown - 1;
    }
    names.join("|")
}

/// Human-readable name for a known RSPAN section kind
/// (docs/span-segment-format.md).
fn rspan_section_kind_name(kind: u32) -> &'static str {
    match kind {
        ravel_rspan::footer::kind::BLOCKS => "BLOCKS",
        ravel_rspan::footer::kind::SKIP_IDX => "SKIP_IDX",
        ravel_rspan::footer::kind::BLOOM => "BLOOM",
        _ => "UNKNOWN",
    }
}

/// Human-readable name for an RSPAN [`StatusCode`] (docs/span-segment-format.md
/// `status_code` column: 0=unset 1=ok 2=error). The names mirror the
/// `StatusCode` variants so the per-record listing cannot drift from the stored
/// status byte definitions.
fn rspan_status_code_name(code: ravel_rspan::StatusCode) -> &'static str {
    match code {
        ravel_rspan::StatusCode::Unset => "unset",
        ravel_rspan::StatusCode::Ok => "ok",
        ravel_rspan::StatusCode::Error => "error",
    }
}

/// Human-readable name for an RSPAN section `comp` tag (0=none, 2=zstd).
fn rspan_comp_name(comp: u8) -> &'static str {
    match comp {
        ravel_rspan::footer::COMP_NONE => "none",
        ravel_rspan::footer::COMP_ZSTD => "zstd",
        _ => "unknown",
    }
}

/// Inspects a whole RSPAN object (docs/span-segment-format.md): footer identity
/// and summary, the section table, the interval-aware skip index (one line per
/// block), and, for a v3 object (ADR-0054), the BLOOM section's coverage (the
/// block count it spans, not its bits) and one line per decoded span record
/// carrying the `service_name` column value alongside the other span columns.
/// Every decode is the reader's own path, so a corrupt SKIP_IDX, BLOOM, or block
/// surfaces as a typed error with a non-zero exit, never a panic.
fn rspan_inspect(bytes: &[u8]) -> anyhow::Result<()> {
    let footer = ravel_rspan::open(bytes)
        .map_err(|err| anyhow::anyhow!("failed to parse rspan segment: {err}"))?;

    println!("total_size: {}", bytes.len());
    println!("version: {}", ravel_rspan::footer::VERSION);
    println!("signal: {}", ravel_rspan::footer::SIGNAL_SPANS);
    println!("tenant_hash: {}", hex::encode(footer.tenant_hash));
    println!("shard: {}", footer.shard);
    println!("writer_id: {}", hex::encode(footer.writer_id));
    println!("writer_epoch: {}", footer.writer_epoch);
    println!("writer_seq: {}", footer.writer_seq);
    println!("min_start_ts_ns: {}", footer.min_start_ts_ns);
    println!("max_end_ts_ns: {}", footer.max_end_ts_ns);
    println!("record_count: {}", footer.record_count);
    println!("block_count: {}", footer.block_count);
    println!("min_trace_id: {}", hex::encode(footer.min_trace_id));
    println!("max_trace_id: {}", hex::encode(footer.max_trace_id));
    println!("level: {}", footer.level);
    println!("input_set_hash: {}", hex::encode(&footer.input_set_hash));
    println!("part_index: {}", footer.part_index);
    println!("sections:");
    for section in &footer.sections {
        println!(
            "  kind={} name={} offset={} len={} comp={} uncompressed_len={}",
            section.kind,
            rspan_section_kind_name(section.kind),
            section.offset,
            section.len,
            rspan_comp_name(section.comp),
            section.uncomp_len,
        );
    }

    // The skip index is reconstructed through ravel-rspan's own `read_section`
    // (the reader's crc-verify-and-decompress path), so the inspector applies the
    // exact `Corrupted` discipline the reader does.
    let skip_desc = footer
        .section(ravel_rspan::footer::kind::SKIP_IDX)
        .ok_or_else(|| anyhow::anyhow!("missing SKIP_IDX section"))?;
    let skip_raw = ravel_rspan::read_section(
        bytes,
        skip_desc,
        ravel_rspan::footer::DEFAULT_MAX_SECTION_UNCOMP,
    )
    .map_err(|err| anyhow::anyhow!("failed to read skip index section: {err}"))?;
    let skip =
        ravel_rspan::skip_index::SkipIndex::decode(&skip_raw, ravel_rspan::reader::MAX_BLOCKS)
            .map_err(|err| anyhow::anyhow!("failed to decode skip index: {err}"))?;
    println!("skip_index ({} block(s)):", skip.blocks.len());
    for (i, entry) in skip.blocks.iter().enumerate() {
        println!(
            "  block[{i}] offset={} len={} crc32c={:08x} record_count={} \
             trace_id_range=[{}, {}] start_ts_min={} end_ts_max={} \
             duration_ns=[{}, {}] status_mask={:03b} ({})",
            entry.block_offset,
            entry.block_len,
            entry.block_crc32c,
            entry.record_count,
            hex::encode(entry.min_trace_id),
            hex::encode(entry.max_trace_id),
            entry.min_start_ts,
            entry.max_end_ts,
            entry.min_duration_ns,
            entry.max_duration_ns,
            entry.status_mask,
            rspan_status_mask_names(entry.status_mask),
        );
    }

    // BLOOM section and service_name column (v3, ADR-0054). Both are present
    // only from v3 on, so gate on the BLOOM section descriptor: an older object
    // without it still inspects its footer, sections, and skip index above.
    // Decoding goes through `RspanReader`, the reader's own crc-verify path, so
    // a corrupt BLOOM or block is a typed error with a non-zero exit here, the
    // same discipline the skip index decode above applies.
    if footer.section(ravel_rspan::footer::kind::BLOOM).is_some() {
        let reader = ravel_rspan::RspanReader::new(bytes, &ravel_rspan::RspanConfig::default())
            .map_err(|err| anyhow::anyhow!("failed to open rspan reader: {err}"))?;
        // The bloom carries one entry per block (the reader verifies this count
        // against the skip index at open time). Report that coverage, not the
        // bloom bits themselves.
        let bloom = reader
            .bloom()
            .map_err(|err| anyhow::anyhow!("failed to parse bloom section: {err}"))?;
        println!("bloom ({} block(s))", bloom.len());

        // One line per span, in the object's stored (trace_id, start_ts) order.
        // `service_name` is the column lifted out of the attribute map (v3,
        // ADR-0054); print it as its own column. The remaining attributes come
        // from the v4 per-key columns and the `attrs_raw` overflow, reassembled
        // by the reader; list them with `service.name` and the events blob
        // filtered out (events are printed structurally below). Span events
        // (v4, ADR-0045 decision 3) are decoded from the reconstructed
        // `_events_raw` value back into their nested fields.
        let (records, _stats) = reader
            .scan(&ravel_rspan::SpanQuery::ts_range(i64::MIN, i64::MAX))
            .map_err(|err| anyhow::anyhow!("failed to scan span records: {err}"))?;
        println!("records ({}):", records.len());
        for (i, rec) in records.iter().enumerate() {
            let service = ravel_rspan::record::service_name_of(&rec.attrs).unwrap_or("");
            let events = rec
                .attrs
                .iter()
                .find(|(k, _)| k == ravel_rspan::record::EVENTS_RAW_KEY)
                .and_then(|(_, v)| ravel_rspan::record::parse_events(v))
                .unwrap_or_default();
            let attrs = rec
                .attrs
                .iter()
                .filter(|(k, _)| {
                    k != ravel_rspan::record::SERVICE_NAME_KEY
                        && k != ravel_rspan::record::EVENTS_RAW_KEY
                })
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "  record[{i}] trace_id={} span_id={} parent_span_id={} name={} \
                 start_ts_ns={} end_ts_ns={} status={} status_message={} \
                 service_name={} attrs={} events={}",
                hex::encode(rec.trace_id),
                hex::encode(rec.span_id),
                rec.parent_span_id.map(hex::encode).unwrap_or_default(),
                rec.name,
                rec.start_ts_ns,
                rec.end_ts_ns,
                rspan_status_code_name(rec.status_code),
                rec.status_message.as_deref().unwrap_or(""),
                service,
                attrs,
                events.len(),
            );
            for (j, ev) in events.iter().enumerate() {
                println!(
                    "    event[{j}] ts_ns={} name={} attrs_blob={}",
                    ev.ts_ns,
                    ev.name,
                    hex::encode(&ev.attrs_blob),
                );
            }
        }
    }

    Ok(())
}

fn commit_decode(bytes: &[u8]) -> anyhow::Result<()> {
    let record = ravel_commit::record::decode(bytes)
        .map_err(|err| anyhow::anyhow!("failed to decode commit record: {err}"))?;
    println!("format_version: {}", record.format_version);
    println!("tenant_hash: {}", hex::encode(&record.tenant_hash));
    println!("signal: {}", record.signal);
    println!("shard: {}", record.shard);
    println!("writer_id: {}", record.writer_id);
    println!("writer_epoch: {}", record.writer_epoch);
    println!("writer_seq: {}", record.writer_seq);
    println!("object_key: {}", record.object_key);
    println!("object_size: {}", record.object_size);
    println!("content_hash: {}", hex::encode(&record.content_hash));
    println!("sample_count: {}", record.sample_count);
    println!("series_count: {}", record.series_count);
    println!("min_event_ts_ns: {}", record.min_event_ts_ns);
    println!("max_event_ts_ns: {}", record.max_event_ts_ns);
    println!("min_ingest_ts_ns: {}", record.min_ingest_ts_ns);
    println!("max_ingest_ts_ns: {}", record.max_ingest_ts_ns);
    println!("segment_format_version: {}", record.segment_format_version);
    println!("created_unix_ns: {}", record.created_unix_ns);
    println!("ingest_hour_bucket: {}", record.ingest_hour_bucket);
    Ok(())
}

async fn catalog_list(
    store_args: &store::StoreArgs,
    tenant: &str,
    hours: i64,
    shard_count: u32,
) -> anyhow::Result<()> {
    let store = store::build_store(store_args)?;
    let selection = store_args.selection();
    let tenant_hash = TenantId::new(tenant).hash();
    selection.print_header();
    store::require_tenant_data_present(
        selection,
        store.as_ref(),
        "catalog list",
        tenant,
        &tenant_hash,
    )
    .await?;
    let catalog_config = ravel_catalog::CatalogConfig {
        shard_count,
        ..ravel_catalog::CatalogConfig::default()
    };
    // Enforcing, matching the server's query path (`ravel_server::query`): an
    // enforcing resolve reads the tenant's real shard-generation history and
    // scans the per-hour generation-aware shard set, instead of short-circuiting
    // to the single implicit generation 0 and under-scanning `0..--shards` after
    // a reshard-increase (ADR-0052 section 4, Finding 3).
    let catalog = ravel_catalog::Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();

    let now = now_ns()?;
    let range = TimeRange {
        start_ns: now.saturating_sub(hours.saturating_mul(NS_PER_HOUR)),
        end_ns: now,
    };
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now)
        .await
        .map_err(|err| anyhow::anyhow!("failed to resolve catalog: {err}"))?;

    for seg in &snapshot.segments {
        println!(
            "{} shard={} samples={} series={} min_event_ts_ns={} max_event_ts_ns={} created_unix_ns={}",
            seg.data_object_key,
            seg.shard,
            seg.sample_count,
            seg.series_count,
            seg.min_event_ts_ns,
            seg.max_event_ts_ns,
            seg.created_unix_ns
        );
    }
    println!("{} segment(s)", snapshot.segments.len());
    Ok(())
}

/// Reclaim pre-namespacing local disk-cache files under `cache_dir` (issue
/// #826). Dry run by default; `apply` deletes. Prints the report in the CLI's
/// existing `key: value` plus indented-list style, then the exit status is the
/// `Result`: an unlistable directory or a failed delete is a nonzero exit.
fn cache_reclaim_legacy(cache_dir: &std::path::Path, apply: bool) -> anyhow::Result<()> {
    let report = ravel_cache::reclaim_legacy(cache_dir, apply).map_err(|err| {
        anyhow::anyhow!(
            "reclaiming legacy cache files under {}: {err}",
            cache_dir.display()
        )
    })?;
    let verb = if report.applied {
        "deleted"
    } else {
        "would delete"
    };
    println!("cache_dir: {}", cache_dir.display());
    println!("apply: {}", report.applied);
    println!("legacy files ({verb}): {}", report.files);
    println!("legacy bytes ({verb}): {}", report.bytes);
    for path in &report.paths {
        println!("  {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use clap::Parser;

    use super::rspan_status_mask_names;
    use super::{BloomScopeCommand, Cli, ClusteringKeyCommand, Command, tenancy};
    use ravel_rspan::skip_index::{STATUS_BIT_ERROR, STATUS_BIT_OK, STATUS_BIT_UNSET};

    /// This unit test binary is built from this binary root, so it links the
    /// `#[global_allocator]` above; an integration test in `tests/` links the
    /// library instead and would not. Read jemalloc's own allocated-bytes stat
    /// and prove it accounts for a fresh large allocation: under glibc malloc
    /// that allocation never touches jemalloc's arenas, so its counter does not
    /// move and this assertion fails. That flip is what makes this a real
    /// runtime check of the installed allocator, not a `cfg` echo.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn binary_runs_under_jemalloc() {
        use tikv_jemalloc_ctl::{epoch, stats};

        epoch::advance().expect("jemalloc epoch mallctl must succeed under jemalloc");
        let before = stats::allocated::read().expect("jemalloc stats.allocated read must succeed");

        let big: Vec<u8> = vec![7u8; 64 * 1024 * 1024];
        std::hint::black_box(big.as_ptr());

        epoch::advance().expect("jemalloc epoch mallctl must succeed under jemalloc");
        let after = stats::allocated::read().expect("jemalloc stats.allocated read must succeed");

        assert!(
            after > before,
            "jemalloc allocated bytes did not grow ({before} -> {after}) across a 64 MiB \
             allocation: the process is not running under the jemalloc global allocator"
        );
        std::hint::black_box(big);
    }

    /// Every subcommand path, space-joined, whose own arguments include
    /// `--tenant-kms-config`.
    fn paths_taking_tenant_kms_config(
        command: &clap::Command,
        prefix: &str,
        out: &mut Vec<String>,
    ) {
        for sub in command.get_subcommands() {
            let path = if prefix.is_empty() {
                sub.get_name().to_string()
            } else {
                format!("{prefix} {}", sub.get_name())
            };
            if sub
                .get_arguments()
                .any(|arg| arg.get_long() == Some("tenant-kms-config"))
            {
                out.push(path.clone());
            }
            paths_taking_tenant_kms_config(sub, &path, out);
        }
    }

    /// Issue #2363: `--tenant-kms-config` is taken by exactly the commands
    /// that write tenant data under the Maintain credential, and by nothing
    /// else. Every Admin command that writes a control record (`hold set`,
    /// `erase submit`, `commit reconstruct`, `provision`, `tenant
    /// parquet-grant`, `typed-attr-column set`, `clustering-key`, `bloom-scope`)
    /// has no way to receive it, so its writes stay under the bucket's default
    /// encryption, as ADR-0055's Decrypt-only Admin role requires.
    ///
    /// Non-vacuity: drop the `tenant_kms` field from `MaintainCommand::Migrate`
    /// and the set loses `maintain migrate`; flatten `TenantKmsArgs` into
    /// `HoldCommand::Set` and it gains `hold set`. Either fails the equality.
    #[test]
    fn only_maintain_credential_data_writers_take_tenant_kms_config() {
        use clap::CommandFactory;
        let mut got = Vec::new();
        paths_taking_tenant_kms_config(&Cli::command(), "", &mut got);
        got.sort();
        assert_eq!(
            got,
            [
                "catalog fold",
                "maintain compact-bucket",
                "maintain compact-tenant",
                "maintain migrate",
            ]
        );
    }

    /// `load --signal` defaults to logs and accepts all three signal names
    /// (ADR-1751 decision 1). The default is what keeps every pre-ADR-1751
    /// invocation, which names no signal at all, loading into logs.
    ///
    /// Non-vacuity (prove-the-test): the pre-change tree had no `signal`
    /// field on `Command::Load`, and `--signal metrics` exited 2 with
    /// "unexpected argument '--signal' found"; this test's second block fails
    /// to parse against it.
    #[test]
    fn load_signal_flag_defaults_to_logs_and_accepts_every_signal() {
        let base = [
            "ravel",
            "load",
            "--parquet",
            "hits.parquet",
            "--tenant",
            "acme",
            "--mapping",
            "hits.toml",
        ];

        let Command::Load { signal, .. } = Cli::try_parse_from(base)
            .expect("a load with no --signal parses")
            .command
        else {
            panic!("expected the load subcommand");
        };
        assert_eq!(
            signal,
            ravel_cli::maintain::SignalArg::Logs,
            "an omitted --signal keeps the pre-ADR-1751 behaviour: load into logs"
        );

        for (name, want) in [
            ("metrics", ravel_cli::maintain::SignalArg::Metrics),
            ("logs", ravel_cli::maintain::SignalArg::Logs),
            ("spans", ravel_cli::maintain::SignalArg::Spans),
        ] {
            let Command::Load { signal, .. } =
                Cli::try_parse_from(base.iter().copied().chain(["--signal", name]))
                    .unwrap_or_else(|e| panic!("--signal {name} parses: {e}"))
                    .command
            else {
                panic!("expected the load subcommand");
            };
            assert_eq!(signal, want, "--signal {name} reaches the field");
        }
    }

    /// `clustering-key set --bucket-width` and `bloom-scope set --scope` take
    /// the spellings the guide documents, each reaching its own variant, and
    /// refuse clap's derived spelling of a variant name.
    #[test]
    fn storage_layout_value_names_parse_to_their_variants() {
        use ravel_cli::storage_layout::{BloomScopeArg, BucketWidthArg};
        let key_set = |width: &str| {
            Cli::try_parse_from([
                "ravel",
                "clustering-key",
                "set",
                "--tenant",
                "t",
                "--column",
                "k",
                "--bucket-width",
                width,
                "--readers-rolled-out",
            ])
            .map(|cli| match cli.command {
                Command::ClusteringKey {
                    command: ClusteringKeyCommand::Set { bucket_width, .. },
                } => bucket_width,
                _ => panic!("expected clustering-key set"),
            })
        };
        for (name, want) in [
            ("1h", BucketWidthArg::OneHour),
            ("6h", BucketWidthArg::SixHours),
            ("1d", BucketWidthArg::OneDay),
        ] {
            let got = key_set(name).unwrap_or_else(|e| panic!("--bucket-width {name}: {e}"));
            assert_eq!(got, want, "--bucket-width {name}");
        }
        assert!(key_set("six-hours").is_err());

        let scope_set = |scope: &str| {
            Cli::try_parse_from([
                "ravel",
                "bloom-scope",
                "set",
                "--tenant",
                "t",
                "--scope",
                scope,
                "--readers-rolled-out",
            ])
            .map(|cli| match cli.command {
                Command::BloomScope {
                    command: BloomScopeCommand::Set { scope, .. },
                } => scope,
                _ => panic!("expected bloom-scope set"),
            })
        };
        for (name, want) in [
            ("all", BloomScopeArg::All),
            ("undeclared", BloomScopeArg::Undeclared),
            ("text", BloomScopeArg::Text),
        ] {
            let got = scope_set(name).unwrap_or_else(|e| panic!("--scope {name}: {e}"));
            assert_eq!(got, want, "--scope {name}");
        }
        assert!(scope_set("none").is_err());
    }

    /// The shipped write-concurrency defaults, read where an operator meets
    /// them: `ravel load` with neither window flag given (issue #800). The
    /// constants in `load.rs` are only the shipped behaviour if the clap
    /// attributes actually reference them, and a `default_value_t = 1` literal
    /// on either flag would leave the library constant correct and the binary
    /// serial.
    ///
    /// Non-vacuity (prove-the-test): the pre-change tree had
    /// `#[arg(long, default_value_t = 1)] pipeline_depth`, and this test's
    /// `pipeline_depth == DEFAULT_PIPELINE_DEPTH` assertion fails against it
    /// (1 against 4).
    #[test]
    fn load_write_window_flags_default_to_the_documented_constants() {
        let cli = Cli::try_parse_from([
            "ravel",
            "load",
            "--parquet",
            "hits.parquet",
            "--tenant",
            "acme",
            "--mapping",
            "hits.toml",
        ])
        .expect("a load invocation with no window flags parses");

        let Command::Load {
            pipeline_depth,
            max_inflight_flushes,
            ..
        } = cli.command
        else {
            panic!("expected the load subcommand");
        };

        assert_eq!(
            pipeline_depth,
            ravel_cli::load::DEFAULT_PIPELINE_DEPTH,
            "--pipeline-depth must default to DEFAULT_PIPELINE_DEPTH"
        );
        assert_eq!(
            max_inflight_flushes,
            ravel_cli::load::DEFAULT_MAX_INFLIGHT_FLUSHES,
            "--max-inflight-flushes must default to DEFAULT_MAX_INFLIGHT_FLUSHES"
        );
    }

    /// `--max-flush-delay` is absent by default (so the loader keeps the
    /// router's own age-trigger default) and, when given, parses humantime into
    /// the `Option<Duration>` the load handler threads on (issue #801).
    #[test]
    fn max_flush_delay_flag_is_optional_and_parses_humantime() {
        let base = [
            "ravel",
            "load",
            "--parquet",
            "hits.parquet",
            "--tenant",
            "acme",
            "--mapping",
            "hits.toml",
        ];

        let Command::Load {
            max_flush_delay, ..
        } = Cli::try_parse_from(base)
            .expect("a load with no --max-flush-delay parses")
            .command
        else {
            panic!("expected the load subcommand");
        };
        assert_eq!(
            max_flush_delay, None,
            "an omitted --max-flush-delay leaves the router default in place"
        );

        let Command::Load {
            max_flush_delay, ..
        } = Cli::try_parse_from(base.iter().copied().chain(["--max-flush-delay", "10m"]))
            .expect("a load with --max-flush-delay 10m parses")
            .command
        else {
            panic!("expected the load subcommand");
        };
        assert_eq!(
            max_flush_delay,
            Some(std::time::Duration::from_secs(600)),
            "--max-flush-delay 10m reaches the field as 600s"
        );
    }

    /// `--zstd-level` defaults to 3, carries a negative level, and is refused
    /// at parse time outside zstd's range with the typed error's message.
    #[test]
    fn zstd_level_flag_defaults_to_3_and_refuses_out_of_range() {
        let base = [
            "ravel",
            "load",
            "--parquet",
            "hits.parquet",
            "--tenant",
            "acme",
            "--mapping",
            "hits.toml",
        ];
        let level_of = |extra: &[&str]| match Cli::try_parse_from(
            base.iter().copied().chain(extra.iter().copied()),
        )
        .map(|cli| cli.command)
        {
            Ok(Command::Load { zstd_level, .. }) => Ok(zstd_level.get()),
            Ok(_) => panic!("expected the load subcommand"),
            Err(e) => Err(e.to_string()),
        };
        assert_eq!(level_of(&[]), Ok(3));
        assert_eq!(level_of(&["--zstd-level", "19"]), Ok(19));
        assert_eq!(level_of(&["--zstd-level", "-5"]), Ok(-5));
        assert_eq!(level_of(&["--zstd-level", "22"]), Ok(22));
        for bad in ["23", "-131073"] {
            let err = level_of(&["--zstd-level", bad]).expect_err("out of range");
            assert!(
                err.contains(&format!(
                    "RLOG zstd level {bad} is outside zstd's accepted range -131072..=22"
                )),
                "{err}"
            );
        }
    }

    /// Issue #1184's classification: `load`, `typed-attr-column set`,
    /// `clustering-key set`/`clear`, `bloom-scope set`, `hold set`/`clear`,
    /// `erase submit`, `provision adopt`/`reshard`, `gc-config
    /// set`, `commit reconstruct`, `catalog fold`, and the mutating `maintain`
    /// subcommands write; their `show`/`list`/`status`/`decode`/`inspect`
    /// siblings do not, and neither do the bucket-root writers `store
    /// qualify` and `tenant token upsert`/`revoke` (see `command_is_write`'s
    /// comments for why the latter two are deliberately left ungated).
    ///
    /// `maintain migrate --dry-run` runs only the read-only re-audit, so it is
    /// a read even with `--reencode-compaction-parts`: replacing
    /// `MaintainCommand::Migrate { dry_run, .. } => !dry_run,` in
    /// `command_is_write` with `=> true` fails the dry-run migrate case.
    #[test]
    fn command_is_write_classifies_the_named_shapes() {
        let cases: &[(&[&str], bool)] = &[
            (
                &[
                    "ravel",
                    "load",
                    "--parquet",
                    "h.parquet",
                    "--tenant",
                    "t",
                    "--mapping",
                    "m.toml",
                ],
                true,
            ),
            (&["ravel", "typed-attr-column", "set", "t", "k:str"], true),
            (&["ravel", "typed-attr-column", "show", "t"], false),
            (
                &[
                    "ravel",
                    "clustering-key",
                    "set",
                    "--tenant",
                    "t",
                    "--column",
                    "k",
                    "--bucket-width",
                    "6h",
                    "--readers-rolled-out",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "clustering-key",
                    "clear",
                    "--tenant",
                    "t",
                    "--readers-rolled-out",
                ],
                true,
            ),
            (&["ravel", "clustering-key", "show", "--tenant", "t"], false),
            (
                &[
                    "ravel",
                    "bloom-scope",
                    "set",
                    "--tenant",
                    "t",
                    "--scope",
                    "text",
                    "--readers-rolled-out",
                ],
                true,
            ),
            (&["ravel", "bloom-scope", "show", "--tenant", "t"], false),
            (
                &["ravel", "hold", "set", "--tenant", "t", "--scope", "t/x/"],
                true,
            ),
            (
                &["ravel", "hold", "clear", "--tenant", "t", "--scope", "t/x/"],
                true,
            ),
            (&["ravel", "hold", "list", "--tenant", "t"], false),
            (
                &[
                    "ravel",
                    "erase",
                    "submit",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--matcher",
                    "k=v",
                ],
                true,
            ),
            (
                &[
                    "ravel", "erase", "status", "--tenant", "t", "--signal", "logs",
                ],
                false,
            ),
            (
                &[
                    "ravel",
                    "provision",
                    "adopt",
                    "--tenant",
                    "t",
                    "--shards",
                    "4",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "provision",
                    "reshard",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--shard-count",
                    "8",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "gc-config",
                    "set",
                    "--protection-horizon",
                    "25h",
                    "--grace",
                    "24h",
                    "--max-query-duration",
                    "1h",
                    "--max-flush-lifetime",
                    "1h",
                ],
                true,
            ),
            (&["ravel", "gc-config", "show"], false),
            (&["ravel", "tenancy", "show"], false),
            (&["ravel", "tenancy", "resolve", "t"], false),
            (
                &[
                    "ravel",
                    "commit",
                    "reconstruct",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--shard",
                    "0",
                ],
                true,
            ),
            (&["ravel", "commit", "decode", "some-key"], false),
            (&["ravel", "catalog", "fold", "--tenant", "t"], true),
            (&["ravel", "catalog", "list", "--tenant", "t"], false),
            (
                &[
                    "ravel", "maintain", "sweep", "--tenant", "t", "--signal", "logs", "--shard",
                    "0",
                ],
                true,
            ),
            // `--dry-run` reports the plan and mutates nothing, so it is a read
            // even on the commands whose non-dry form writes. Gating it would
            // stop an operator inspecting the plan on a marker-less bucket.
            (
                &[
                    "ravel",
                    "maintain",
                    "sweep",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--shard",
                    "0",
                    "--dry-run",
                ],
                false,
            ),
            (
                &[
                    "ravel",
                    "maintain",
                    "compact-tenant",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--dry-run",
                ],
                false,
            ),
            (
                &[
                    "ravel",
                    "maintain",
                    "migrate",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--reencode-compaction-parts",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "maintain",
                    "migrate",
                    "--tenant",
                    "t",
                    "--signal",
                    "logs",
                    "--reencode-compaction-parts",
                    "--dry-run",
                ],
                false,
            ),
            (
                &[
                    "ravel", "maintain", "status", "--tenant", "t", "--signal", "logs", "--shard",
                    "0", "--hour", "0",
                ],
                false,
            ),
            (&["ravel", "store", "qualify"], false),
            (
                &[
                    "ravel",
                    "tenant",
                    "token",
                    "upsert",
                    "--deployment-key-file",
                    "k",
                    "--token",
                    "tok",
                    "--tenant",
                    "t",
                ],
                false,
            ),
            (
                &[
                    "ravel",
                    "tenant",
                    "token",
                    "list",
                    "--deployment-key-file",
                    "k",
                ],
                false,
            ),
            (
                &[
                    "ravel", "parquet", "sweep", "--tenant", "t", "--grace", "1h",
                ],
                true,
            ),
            (&["ravel", "parquet", "ls", "--tenant", "t"], false),
            (
                &[
                    "ravel", "parquet", "repair", "--tenant", "t", "--table", "hits",
                ],
                false,
            ),
            (
                &[
                    "ravel", "parquet", "repair", "--tenant", "t", "--table", "hits", "--delete",
                ],
                true,
            ),
            (
                &["ravel", "parquet", "repair", "--tenant", "t", "--stray"],
                false,
            ),
            (
                &[
                    "ravel", "parquet", "repair", "--tenant", "t", "--stray", "--delete",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "parquet",
                    "repair",
                    "--tenant",
                    "t",
                    "--stray",
                    "--delete",
                    "--include-reserved-names",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "parquet",
                    "repair",
                    "--tenant",
                    "t",
                    "--table",
                    "hits",
                    "--delete-version",
                    "5",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "tenant",
                    "parquet-grant",
                    "add",
                    "--tenant",
                    "t",
                    "--location",
                    "s3://lake/data/",
                    "--profile",
                    "lake",
                ],
                true,
            ),
            (
                &[
                    "ravel",
                    "tenant",
                    "parquet-grant",
                    "remove",
                    "--tenant",
                    "t",
                    "--location",
                    "s3://lake/data/",
                ],
                true,
            ),
            (
                &["ravel", "tenant", "parquet-grant", "ls", "--tenant", "t"],
                false,
            ),
        ];
        for (args, expect_write) in cases {
            let cli =
                Cli::try_parse_from(*args).unwrap_or_else(|e| panic!("{args:?} must parse: {e}"));
            assert_eq!(
                super::command_is_write(&cli.command),
                *expect_write,
                "{args:?} classified as write={}, expected {}",
                super::command_is_write(&cli.command),
                expect_write
            );
        }
    }

    /// `parquet repair` takes exactly one of `--table` and `--stray`, and
    /// `--delete-version` only with `--table`.
    #[test]
    fn parquet_repair_takes_a_table_or_stray_and_never_both() {
        let base = ["ravel", "parquet", "repair", "--tenant", "t"];
        for tail in [
            &[][..],
            &["--delete"][..],
            &["--stray", "--table", "hits"][..],
            &["--stray", "--delete-version", "5"][..],
            &["--stray", "--include-reserved-names"][..],
            &["--table", "hits", "--delete", "--include-reserved-names"][..],
        ] {
            let args: Vec<&str> = base.iter().chain(tail).copied().collect();
            assert!(Cli::try_parse_from(&args).is_err(), "{args:?} parsed");
        }
        for (tail, include) in [
            (&["--stray", "--delete"][..], false),
            (
                &["--stray", "--delete", "--include-reserved-names"][..],
                true,
            ),
        ] {
            let args: Vec<&str> = base.iter().chain(tail).copied().collect();
            let cli = Cli::try_parse_from(&args).expect("parse");
            assert!(
                matches!(
                    cli.command,
                    Command::Parquet {
                        command: super::ParquetCommand::Repair {
                            table: None,
                            stray: true,
                            delete: true,
                            delete_version: None,
                            include_reserved_names,
                            ..
                        }
                    } if include_reserved_names == include
                ),
                "{args:?}"
            );
        }
    }

    /// Issue #1184, end to end against the exact gate `main` runs before
    /// dispatch: `hold set` (a writing shape that hashes a tenant) against a
    /// fresh, marker-less bucket with no scheme flag refuses with the
    /// server's own `FreshBucketNeedsKey` wording, and the store holds zero
    /// objects both before and after the attempt -- proving the refusal
    /// happens before any write, not just that an error was raised.
    ///
    /// Non-vacuity: before this fix, `command_is_write` did not exist and
    /// every tenant-hashing command (write or not) called plain
    /// `resolve_scheme`, whose `Unspecified` arm returns
    /// `Ok(TenantHashScheme::V1Unkeyed)`. Reverting the `if
    /// super::command_is_write(&cli.command) { resolve_scheme_for_write }
    /// else { resolve_scheme }` branch below to always call `resolve_scheme`
    /// turns this test's `expect_err` into a panic on `Ok`.
    #[tokio::test]
    async fn hold_set_refuses_and_writes_nothing_on_marker_less_bucket() {
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::memory::MemoryStore;

        let cli = Cli::try_parse_from([
            "ravel", "hold", "set", "--tenant", "acme", "--scope", "t/x/",
        ])
        .expect("hold set parses");
        let store: std::sync::Arc<dyn ObjectStoreBackend> = std::sync::Arc::new(MemoryStore::new());

        let objects_before = store
            .list("", None)
            .await
            .expect("list succeeds on an empty store")
            .objects
            .len();
        assert_eq!(objects_before, 0);

        let configured = tenancy::configured_scheme_from_flags(None, false)
            .expect("no scheme flags is a valid configuration");
        let gate = if super::command_is_write(&cli.command) {
            tenancy::resolve_scheme_for_write(store.as_ref(), configured).await
        } else {
            tenancy::resolve_scheme(store.as_ref(), configured).await
        };
        let err = gate.expect_err("hold set on a fresh bucket with no scheme flag must refuse");
        assert!(
            err.to_string().contains(
                "this is a fresh bucket and the tenant hash is keyed by default, but no \
                 --tenant-hash-key-file was configured"
            ),
            "err: {err}"
        );

        let objects_after = store
            .list("", None)
            .await
            .expect("list still succeeds")
            .objects
            .len();
        assert_eq!(
            objects_after, 0,
            "the refused hold set must not have written any object"
        );
    }

    /// The read-only sibling in the same command group, `hold list`, keeps
    /// working against the same marker-less bucket with no scheme flag
    /// (issue #1184's explicit requirement): `command_is_write` is false for
    /// it, so it takes the lenient `resolve_scheme` path and gets the
    /// v1-unkeyed default rather than a refusal.
    #[tokio::test]
    async fn hold_list_still_succeeds_on_marker_less_bucket() {
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::memory::MemoryStore;

        let cli = Cli::try_parse_from(["ravel", "hold", "list", "--tenant", "acme"])
            .expect("hold list parses");
        assert!(!super::command_is_write(&cli.command));

        let store: std::sync::Arc<dyn ObjectStoreBackend> = std::sync::Arc::new(MemoryStore::new());
        let configured = tenancy::configured_scheme_from_flags(None, false)
            .expect("no scheme flags is a valid configuration");
        tenancy::resolve_scheme(store.as_ref(), configured)
            .await
            .expect("a read-only command must keep working on a fresh, marker-less bucket");
    }

    /// `gc-config set` writes `sys/gc`, a bucket-root object with no tenant
    /// prefix, so it never reaches the tenant-hashing gate above; it still
    /// must clear the write gate on its own (the `else if
    /// command_is_write(&cli.command)` arm in `main`), since the invariant
    /// is "does the server accept this bucket", not "does this command hash
    /// a tenant".
    #[tokio::test]
    async fn gc_config_set_refuses_on_marker_less_bucket() {
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::memory::MemoryStore;

        let cli = Cli::try_parse_from([
            "ravel",
            "gc-config",
            "set",
            "--protection-horizon",
            "25h",
            "--grace",
            "24h",
            "--max-query-duration",
            "1h",
            "--max-flush-lifetime",
            "1h",
        ])
        .expect("gc-config set parses");
        assert!(!super::command_hashes_tenant(&cli.command));
        assert!(super::command_is_write(&cli.command));

        let store: std::sync::Arc<dyn ObjectStoreBackend> = std::sync::Arc::new(MemoryStore::new());
        let configured = tenancy::configured_scheme_from_flags(None, false)
            .expect("no scheme flags is a valid configuration");
        let err = tenancy::resolve_scheme_for_write(store.as_ref(), configured)
            .await
            .expect_err("gc-config set on a fresh bucket with no scheme flag must refuse");
        assert!(
            err.to_string().contains(
                "this is a fresh bucket and the tenant hash is keyed by default, but no \
                 --tenant-hash-key-file was configured"
            ),
            "err: {err}"
        );

        let objects_after = store
            .list("", None)
            .await
            .expect("list succeeds")
            .objects
            .len();
        assert_eq!(
            objects_after, 0,
            "the refused gc-config set must not have written sys/gc"
        );
    }

    use ravel_cli::maintain::{self, CompactorKnobError};

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    /// The compactor's default claim lease (300 s), what the CLI derives
    /// against when no claim-lease override is given.
    const DEFAULT_LEASE: std::time::Duration = ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION;

    /// Destructure a parsed `maintain compact-tenant` invocation into the three
    /// new memory knobs plus dry-run/flush overrides, panicking on any other
    /// subcommand. Keeps each reachability test to the assertion it is about.
    #[allow(clippy::type_complexity)]
    fn compact_tenant_knobs(
        cli: Cli,
    ) -> (bool, Option<i64>, Option<u64>, Option<u64>, Option<usize>) {
        let Command::Maintain {
            command:
                super::MaintainCommand::CompactTenant {
                    dry_run,
                    max_flush_lifetime,
                    l1_part_memory_target_bytes,
                    max_l1_part_bytes,
                    input_read_concurrency,
                    ..
                },
        } = cli.command
        else {
            panic!("expected the maintain compact-tenant subcommand");
        };
        (
            dry_run,
            max_flush_lifetime,
            l1_part_memory_target_bytes,
            max_l1_part_bytes,
            input_read_concurrency,
        )
    }

    /// Each new memory knob, given on a real `compact-tenant` argument vector,
    /// arrives verbatim in the built `CompactorConfig`, and a fresh
    /// `MergeMemoryTracker` is installed (so the per-phase peak-memory event
    /// `rewrite_and_publish` emits fires for each bucket).
    ///
    /// Distinct values (12345 / 67890 / 7) so a crossed-wire mapping cannot pass.
    ///
    /// Non-vacuity (prove-the-test), each flip named:
    /// - Drop `l1_part_memory_target_bytes` from the `build_compactor_config`
    ///   call (pass `None`): the 12345 assertion fails against the 256 MiB
    ///   default.
    /// - Same for `max_l1_part_bytes` (67890) and `input_read_concurrency` (7).
    /// - Revert `merge_memory_tracker` to `None` in `build_compactor_config`:
    ///   the `is_some()` assertion fails.
    #[test]
    fn compact_tenant_memory_knobs_reach_the_built_config() {
        let cli = Cli::try_parse_from([
            "ravel",
            "maintain",
            "compact-tenant",
            "--tenant",
            "acme",
            "--signal",
            "logs",
            "--l1-part-memory-target-bytes",
            "12345",
            "--max-l1-part-bytes",
            "67890",
            "--input-read-concurrency",
            "7",
        ])
        .expect("a compact-tenant invocation with the memory knobs parses");

        let (dry_run, max_flush, mem_target, max_bytes, concurrency) = compact_tenant_knobs(cli);

        // A 30 GiB host at bucket concurrency 2 would derive 2013265920; the
        // flag must win over that verbatim, unclamped.
        let (config, resolved) = maintain::build_compactor_config(
            dry_run,
            max_flush,
            mem_target,
            max_bytes,
            concurrency,
            None,
            Some(30 * GIB),
            2,
            DEFAULT_LEASE,
        )
        .expect("nonzero knobs build a config");

        assert_eq!(
            config.l1_part_memory_target_bytes, 12345,
            "--l1-part-memory-target-bytes must arrive in the config"
        );
        assert_eq!(
            config.rlog_memory_target_bytes(),
            12345,
            "and reach the RLOG merge too"
        );
        assert_eq!(
            format!("rlog_l1_part_memory_target_bytes: {resolved}"),
            "rlog_l1_part_memory_target_bytes: 12345 (set by flag)",
            "the report line names the flag as the source"
        );
        assert_eq!(
            config.max_l1_part_bytes, 67890,
            "--max-l1-part-bytes must arrive in the config"
        );
        assert_eq!(
            config.rlog_max_l1_part_bytes,
            Some(67890),
            "and reach the RLOG merge's stored-size cap too"
        );
        assert_eq!(config.rlog_stored_target_bytes(), 67890);
        assert_eq!(
            config.input_read_concurrency, 7,
            "--input-read-concurrency must arrive in the config"
        );
        assert!(
            config.merge_memory_tracker.is_some(),
            "a fresh MergeMemoryTracker must be installed so the phase-split event fires"
        );
    }

    /// Without `--l1-part-memory-target-bytes` the memory split target is
    /// derived from the host memory less the overhead reserve (2 GiB from
    /// 8 GiB of memory up, the band the 32 GiB host below is in) and the
    /// 20 GiB merge cursor budget, over `--bucket-concurrency`, capped by the
    /// claim lease (issue #2351). The worked figure is a 32 GiB host at one
    /// merge: `32 - 2 - 20 = 10 GiB`, `/ 8 = 1.25 GiB` (1342177280), where the
    /// 1.46 GiB lease cap does not bind. Each figure is pinned in the config and
    /// in the report line, including the term that bound it.
    ///
    /// Non-vacuity (prove-the-test), each flip named:
    /// - Drop the cursor deduction (`merge_memory_budget_bytes` returning its
    ///   first argument): the 32 GiB row reads 1572864000 (a 30 GiB budget is
    ///   lease-bound), and the 30 GiB row at 2 merges reads 1572864000 as well
    ///   (28 GiB / 8 / 2 is 1.75 GiB, also lease-bound) instead of 939524096.
    /// - Drop the reserve deduction: the 32 GiB row reads 1500 MiB / 1.5 GiB
    ///   (12 GiB / 8) instead of 1.25 GiB.
    /// - Drop the clamp from `derive_l1_part_memory_target`: the 24 GiB row
    ///   reads 268435456 either way, the 128 GiB row 14227079168 (its 106 GiB
    ///   budget / 8) at the long lease instead of the 8 GiB ceiling.
    /// - Ignore `concurrent_merges` there (or pass 1 from
    ///   `build_compactor_config`): the 30 GiB row at 2 merges reads 1 GiB.
    /// - Drop the lease term: the 52 GiB default-lease row reads 4026531840.
    /// - Keep the 256 MiB struct default instead of resolving: every derived
    ///   row reads 268435456.
    #[test]
    fn compact_tenant_memory_target_defaults_to_the_derived_value() {
        let cli = Cli::try_parse_from([
            "ravel",
            "maintain",
            "compact-tenant",
            "--tenant",
            "acme",
            "--signal",
            "logs",
            "--bucket-concurrency",
            "2",
        ])
        .expect("a compact-tenant invocation without the memory knob parses");
        let Command::Maintain {
            command:
                super::MaintainCommand::CompactTenant {
                    l1_part_memory_target_bytes,
                    bucket_concurrency,
                    ..
                },
        } = cli.command
        else {
            panic!("expected the maintain compact-tenant subcommand");
        };
        assert_eq!(l1_part_memory_target_bytes, None);
        assert_eq!(bucket_concurrency, 2);

        let lease_1200s = std::time::Duration::from_secs(1200);
        let lease_1h = std::time::Duration::from_secs(3600);
        for (host, merges, lease, want_bytes, want_line) in [
            // The worked figure: 32 - 2 - 20 = 10 GiB, / 8 = 1.25 GiB.
            (
                Some(32 * GIB),
                1,
                DEFAULT_LEASE,
                1_342_177_280,
                "rlog_l1_part_memory_target_bytes: 1342177280 (resolved from a memory budget of \
                 10737418240 over 1 concurrent merge; bound by the memory share, budget / 8 / \
                 merges)",
            ),
            // 30 - 2 - 20 = 8 GiB over 2 merges, / 8 = 512 MiB each.
            (
                Some(30 * GIB),
                bucket_concurrency,
                DEFAULT_LEASE,
                536_870_912,
                "rlog_l1_part_memory_target_bytes: 536870912 (resolved from a memory budget of \
                 8589934592 over 2 concurrent merges; bound by the memory share, budget / 8 / \
                 merges)",
            ),
            // 24 - 2 - 20 = 2 GiB, / 8 = 256 MiB: the share equals the floor.
            (
                Some(24 * GIB),
                1,
                DEFAULT_LEASE,
                268_435_456,
                "rlog_l1_part_memory_target_bytes: 268435456 (resolved from a memory budget of \
                 2147483648 over 1 concurrent merge; bound by the memory share, budget / 8 / \
                 merges)",
            ),
            // The deductions leave nothing: the budget floors at zero, then the
            // target at 256 MiB.
            (
                Some(20 * GIB),
                1,
                DEFAULT_LEASE,
                268_435_456,
                "rlog_l1_part_memory_target_bytes: 268435456 (resolved from a memory budget of \
                 0 over 1 concurrent merge; bound by the 268435456-byte floor)",
            ),
            // 52 - 2 - 20 = 30 GiB: the share is 3.75 GiB but the 300 s lease
            // supports 1500 MiB.
            (
                Some(52 * GIB),
                1,
                DEFAULT_LEASE,
                1_572_864_000,
                "rlog_l1_part_memory_target_bytes: 1572864000 (resolved from a memory budget of \
                 32212254720 over 1 concurrent merge; bound by the claim lease, 300 s allows a \
                 part of at most 1572864000 bytes)",
            ),
            // The same host with the lease raised to 1200 s: the share binds.
            (
                Some(52 * GIB),
                1,
                lease_1200s,
                4_026_531_840,
                "rlog_l1_part_memory_target_bytes: 4026531840 (resolved from a memory budget of \
                 32212254720 over 1 concurrent merge; bound by the memory share, budget / 8 / \
                 merges)",
            ),
            // 128 - 2 - 20 = 106 GiB: a one-hour lease and a 13.25 GiB share
            // both pass the 8 GiB ceiling.
            (
                Some(128 * GIB),
                1,
                lease_1h,
                8_589_934_592,
                "rlog_l1_part_memory_target_bytes: 8589934592 (resolved from a memory budget of \
                 113816633344 over 1 concurrent merge; bound by the 8589934592-byte ceiling)",
            ),
            (
                None,
                bucket_concurrency,
                DEFAULT_LEASE,
                268_435_456,
                "rlog_l1_part_memory_target_bytes: 268435456 (fallback: the memory budget is \
                 unknown)",
            ),
        ] {
            let (config, resolved) = maintain::build_compactor_config(
                false,
                None,
                l1_part_memory_target_bytes,
                None,
                None,
                None,
                host,
                merges,
                lease,
            )
            .expect("no knobs build a config");
            assert_eq!(
                config.rlog_memory_target_bytes(),
                want_bytes,
                "host {host:?}"
            );
            assert_eq!(
                config.rlog_stored_target_bytes(),
                want_bytes,
                "the RLOG stored-size cap follows the derived target, host {host:?}"
            );
            assert_eq!(
                config.max_l1_part_bytes,
                256 * MIB,
                "the shared cap RSEG reads is untouched, host {host:?}"
            );
            assert_eq!(
                config.l1_part_memory_target_bytes, 268_435_456,
                "the RSPAN merge keeps 256 MiB without the flag, host {host:?}"
            );
            assert_eq!(
                format!("rlog_l1_part_memory_target_bytes: {resolved}"),
                want_line
            );
            assert_eq!(
                maintain::l1_part_memory_target_fallback_note(&resolved).is_some(),
                host.is_none(),
                "the stderr note is printed for the fallback only"
            );
        }
    }

    /// `--no-claim` on `compact-bucket`, and its absence, reach the
    /// `ClaimOptions` the command runs with (issue #1034).
    ///
    /// The flag is the operator's only way to turn advisory claiming off for a
    /// repair run, and nothing else in the suite parses a real argument vector
    /// for it: a wired-up `ClaimOptions` construction over an unwired flag is
    /// the failure this catches.
    ///
    /// Non-vacuity (prove-the-test): unwire the flag by changing
    /// `MaintainCommand::CompactBucket::no_claim`'s `#[arg(long)]` to
    /// `#[arg(skip)]`, and clap no longer accepts it: the `try_parse_from`
    /// carrying `--no-claim` fails with "unexpected argument '--no-claim'
    /// found".
    #[test]
    fn compact_bucket_no_claim_flag_reaches_the_claim_options() {
        let base = [
            "ravel",
            "maintain",
            "compact-bucket",
            "--tenant",
            "acme",
            "--signal",
            "logs",
            "--shard",
            "0",
            "--hour",
            "100",
        ];

        for (argv_tail, want) in [(Vec::new(), false), (vec!["--no-claim"], true)] {
            let cli = Cli::try_parse_from(base.iter().copied().chain(argv_tail.iter().copied()))
                .unwrap_or_else(|e| panic!("compact-bucket {argv_tail:?} parses: {e}"));
            let Command::Maintain {
                command: super::MaintainCommand::CompactBucket { no_claim, .. },
            } = cli.command
            else {
                panic!("expected the maintain compact-bucket subcommand");
            };
            assert_eq!(no_claim, want, "--no-claim {argv_tail:?} reaches the field");
            assert_eq!(
                maintain::ClaimOptions::for_invocation(no_claim).no_claim,
                want,
                "and the ClaimOptions the command is dispatched with",
            );
        }
    }

    /// `--l1-part-memory-target-bytes` and `--max-l1-part-bytes` parse on
    /// `compact-bucket` into their own fields, and are `None` when absent
    /// (issue #2351). The binary-level test in `tests/compact_tenant.rs` pins
    /// that the dispatch passes them on.
    ///
    /// Distinguishing: without the fields on `CompactBucket` (or with
    /// `#[arg(skip)]`), clap rejects the argument vector; with the two long
    /// names swapped, the values arrive crossed (12345 and 67890 differ).
    #[test]
    fn compact_bucket_part_split_flags_parse_into_their_fields() {
        let base = [
            "ravel",
            "maintain",
            "compact-bucket",
            "--tenant",
            "acme",
            "--signal",
            "logs",
            "--shard",
            "0",
            "--hour",
            "100",
        ];
        for (argv_tail, want_memory, want_stored) in [
            (Vec::new(), None, None),
            (
                vec![
                    "--l1-part-memory-target-bytes",
                    "12345",
                    "--max-l1-part-bytes",
                    "67890",
                ],
                Some(12345),
                Some(67890),
            ),
        ] {
            let cli = Cli::try_parse_from(base.iter().copied().chain(argv_tail.iter().copied()))
                .unwrap_or_else(|e| panic!("compact-bucket {argv_tail:?} parses: {e}"));
            let Command::Maintain {
                command:
                    super::MaintainCommand::CompactBucket {
                        l1_part_memory_target_bytes,
                        max_l1_part_bytes,
                        ..
                    },
            } = cli.command
            else {
                panic!("expected the maintain compact-bucket subcommand");
            };
            assert_eq!(l1_part_memory_target_bytes, want_memory, "{argv_tail:?}");
            assert_eq!(max_l1_part_bytes, want_stored, "{argv_tail:?}");
        }
    }

    /// `--no-claim` on `compact-tenant`, and its absence, reach the
    /// `ClaimOptions` the walk runs with (issue #1034). The `compact-bucket`
    /// pin above says why; the flag is declared separately on each subcommand,
    /// so one pin does not cover the other.
    ///
    /// Non-vacuity (prove-the-test): unwire the flag by changing
    /// `MaintainCommand::CompactTenant::no_claim`'s `#[arg(long)]` to
    /// `#[arg(skip)]`, and the `try_parse_from` carrying `--no-claim` fails
    /// with "unexpected argument '--no-claim' found".
    #[test]
    fn compact_tenant_no_claim_flag_reaches_the_claim_options() {
        let base = [
            "ravel",
            "maintain",
            "compact-tenant",
            "--tenant",
            "acme",
            "--signal",
            "logs",
        ];

        for (argv_tail, want) in [(Vec::new(), false), (vec!["--no-claim"], true)] {
            let cli = Cli::try_parse_from(base.iter().copied().chain(argv_tail.iter().copied()))
                .unwrap_or_else(|e| panic!("compact-tenant {argv_tail:?} parses: {e}"));
            let Command::Maintain {
                command: super::MaintainCommand::CompactTenant { no_claim, .. },
            } = cli.command
            else {
                panic!("expected the maintain compact-tenant subcommand");
            };
            assert_eq!(no_claim, want, "--no-claim {argv_tail:?} reaches the field");
            assert_eq!(
                maintain::ClaimOptions::for_invocation(no_claim).no_claim,
                want,
                "and the ClaimOptions the walk is dispatched with",
            );
        }
    }

    /// `--no-claim` on `migrate`, and its absence, reach the `ClaimOptions`
    /// the walk runs with (issue #2199). The flag is declared separately on
    /// each subcommand, so the `compact-bucket` pin above does not cover it.
    ///
    /// Non-vacuity (prove-the-test): unwire the flag by changing
    /// `MaintainCommand::Migrate::no_claim`'s `#[arg(long)]` to
    /// `#[arg(skip)]`, and the `try_parse_from` carrying `--no-claim` fails
    /// with "unexpected argument '--no-claim' found".
    #[test]
    fn migrate_no_claim_flag_reaches_the_claim_options() {
        let base = [
            "ravel", "maintain", "migrate", "--tenant", "acme", "--signal", "logs",
        ];

        for (argv_tail, want) in [(Vec::new(), false), (vec!["--no-claim"], true)] {
            let cli = Cli::try_parse_from(base.iter().copied().chain(argv_tail.iter().copied()))
                .unwrap_or_else(|e| panic!("migrate {argv_tail:?} parses: {e}"));
            let Command::Maintain {
                command: super::MaintainCommand::Migrate { no_claim, .. },
            } = cli.command
            else {
                panic!("expected the maintain migrate subcommand");
            };
            assert_eq!(no_claim, want, "--no-claim {argv_tail:?} reaches the field");
            assert_eq!(
                maintain::ClaimOptions::for_invocation(no_claim).no_claim,
                want,
                "and the ClaimOptions the walk is dispatched with",
            );
        }
    }

    /// `--dry-run` and `--reencode-compaction-parts` on `migrate` are off by
    /// default and each reaches its own field of the switches the walk runs
    /// with.
    ///
    /// Non-vacuity: change `MaintainCommand::Migrate::reencode_compaction_parts`'s
    /// `#[arg(long)]` to `#[arg(skip)]` and the `try_parse_from` carrying
    /// `--reencode-compaction-parts` fails with "unexpected argument".
    #[test]
    fn migrate_dry_run_and_reencode_flags_reach_the_switches() {
        let base = [
            "ravel", "maintain", "migrate", "--tenant", "acme", "--signal", "logs",
        ];
        for (argv_tail, want_dry, want_reencode) in [
            (Vec::new(), false, false),
            (vec!["--dry-run"], true, false),
            (vec!["--reencode-compaction-parts"], false, true),
            (vec!["--dry-run", "--reencode-compaction-parts"], true, true),
        ] {
            let cli = Cli::try_parse_from(base.iter().copied().chain(argv_tail.iter().copied()))
                .unwrap_or_else(|e| panic!("migrate {argv_tail:?} parses: {e}"));
            let Command::Maintain {
                command:
                    super::MaintainCommand::Migrate {
                        dry_run,
                        reencode_compaction_parts,
                        ..
                    },
            } = cli.command
            else {
                panic!("expected the maintain migrate subcommand");
            };
            assert_eq!(
                (dry_run, reencode_compaction_parts),
                (want_dry, want_reencode),
                "{argv_tail:?}"
            );
        }
    }

    /// A zero part-split byte target is refused with its typed error, per each
    /// field's own doc (the byte budget a part is closed at; zero closes a part
    /// before anything accumulates).
    ///
    /// Non-vacuity (prove-the-test), each flip named:
    /// - Remove the `== Some(0)` guard for `l1_part_memory_target_bytes` in
    ///   `build_compactor_config`: this `ZeroL1PartMemoryTarget` assertion fails
    ///   (it builds `Ok`), with or without a known host memory to derive from.
    /// - Remove the matching guard for `max_l1_part_bytes`: the
    ///   `ZeroMaxL1PartBytes` assertion fails.
    #[test]
    fn compact_tenant_zero_byte_targets_are_refused() {
        for host in [None, Some(30 * GIB)] {
            assert_eq!(
                maintain::build_compactor_config(
                    false,
                    None,
                    Some(0),
                    None,
                    None,
                    None,
                    host,
                    1,
                    DEFAULT_LEASE
                )
                .expect_err("--l1-part-memory-target-bytes 0 must be refused"),
                CompactorKnobError::ZeroL1PartMemoryTarget,
            );
        }
        assert_eq!(
            maintain::build_compactor_config(
                false,
                None,
                None,
                Some(0),
                None,
                None,
                None,
                1,
                DEFAULT_LEASE
            )
            .expect_err("--max-l1-part-bytes 0 must be refused"),
            CompactorKnobError::ZeroMaxL1PartBytes,
        );
    }

    /// `--compaction-zstd-level` on both `compact-bucket` and `compact-tenant`
    /// arrives in the built `CompactorConfig::rlog_zstd_level`, its absence
    /// leaves the compactor default (9), and a level outside 1..=22 is refused
    /// with the typed error before any store access.
    ///
    /// Non-vacuity (prove-the-test), each flip named:
    /// - Pass `None` for `rlog_zstd_level` in `compact_to`'s
    ///   `build_compactor_config` call: unreachable from this parse test, so
    ///   the per-subcommand field match below is what pins the flag; changing
    ///   either subcommand's `#[arg(long, value_name = "LEVEL")]` to
    ///   `#[arg(skip)]` fails its `try_parse_from` with "unexpected argument".
    /// - Drop the `if let Some(level) = rlog_zstd_level` block from
    ///   `build_compactor_config`: the `== 4` assertion reads the default 9.
    /// - Drop the `validate_rlog_zstd_level` call there: the 0 and 23 refusals
    ///   build `Ok`.
    #[test]
    fn compaction_zstd_level_flag_reaches_the_built_config() {
        let bucket = Cli::try_parse_from([
            "ravel",
            "maintain",
            "compact-bucket",
            "--tenant",
            "acme",
            "--signal",
            "logs",
            "--shard",
            "0",
            "--hour",
            "100",
            "--compaction-zstd-level",
            "4",
        ])
        .expect("compact-bucket with --compaction-zstd-level parses");
        let Command::Maintain {
            command:
                super::MaintainCommand::CompactBucket {
                    compaction_zstd_level,
                    ..
                },
        } = bucket.command
        else {
            panic!("expected the maintain compact-bucket subcommand");
        };
        assert_eq!(compaction_zstd_level, Some(4));
        let tenant = Cli::try_parse_from([
            "ravel",
            "maintain",
            "compact-tenant",
            "--tenant",
            "acme",
            "--signal",
            "logs",
            "--compaction-zstd-level",
            "4",
        ])
        .expect("compact-tenant with --compaction-zstd-level parses");
        let Command::Maintain {
            command:
                super::MaintainCommand::CompactTenant {
                    compaction_zstd_level: tenant_level,
                    ..
                },
        } = tenant.command
        else {
            panic!("expected the maintain compact-tenant subcommand");
        };
        assert_eq!(tenant_level, Some(4));

        let (config, _) = maintain::build_compactor_config(
            false,
            None,
            None,
            None,
            None,
            compaction_zstd_level,
            None,
            1,
            DEFAULT_LEASE,
        )
        .expect("level 4 builds a config");
        assert_eq!(
            config.rlog_zstd_level, 4,
            "the flag must arrive in the config"
        );
        let (default, _) = maintain::build_compactor_config(
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            1,
            DEFAULT_LEASE,
        )
        .expect("no flag builds a config");
        assert_eq!(
            default.rlog_zstd_level, 9,
            "no flag keeps the compactor default"
        );
        for level in [0, 23] {
            assert_eq!(
                maintain::build_compactor_config(
                    false,
                    None,
                    None,
                    None,
                    None,
                    Some(level),
                    None,
                    1,
                    DEFAULT_LEASE
                )
                .expect_err("an out-of-range level must be refused"),
                CompactorKnobError::InvalidRlogZstdLevel(ravel_maintain::RlogZstdLevelError {
                    level
                }),
            );
        }
    }

    /// `--input-read-concurrency 0` is NOT refused: its config-field doc states
    /// "Values below 1 are treated as 1," so the CLI passes zero through rather
    /// than enforcing more than the doc states (the merge itself clamps with
    /// `.max(1)`). This pins that deliberate asymmetry with the byte targets.
    ///
    /// Non-vacuity (prove-the-test): add an `input_read_concurrency == 0` guard
    /// to `build_compactor_config` returning an error, and this `Ok` assertion
    /// fails.
    #[test]
    fn compact_tenant_zero_input_read_concurrency_is_allowed() {
        let (config, _) = maintain::build_compactor_config(
            false,
            None,
            None,
            None,
            Some(0),
            None,
            None,
            1,
            DEFAULT_LEASE,
        )
        .expect("zero input-read-concurrency is tolerated, not refused");
        assert_eq!(
            config.input_read_concurrency, 0,
            "zero passes through unchanged; the merge clamps below-1 to 1 itself"
        );
    }

    /// `cache reclaim-legacy` parses its `--cache-dir` and defaults `--apply`
    /// to false (dry run), flipping to true when the flag is given (issue
    /// #826).
    ///
    /// Non-vacuity (prove-the-test): add `default_value_t = true` to the
    /// `apply` arg, and the no-`--apply` assertion (`apply == false`) fails.
    #[test]
    fn cache_reclaim_legacy_parses_cache_dir_and_apply() {
        use super::CacheCommand;

        let Command::Cache {
            command: CacheCommand::ReclaimLegacy { cache_dir, apply },
        } = Cli::try_parse_from([
            "ravel",
            "cache",
            "reclaim-legacy",
            "--cache-dir",
            "/var/cache/rv",
        ])
        .expect("a reclaim-legacy invocation parses")
        .command
        else {
            panic!("expected the cache reclaim-legacy subcommand");
        };
        assert_eq!(cache_dir, std::path::PathBuf::from("/var/cache/rv"));
        assert!(!apply, "--apply defaults to false (dry run)");

        let Command::Cache {
            command: CacheCommand::ReclaimLegacy { apply, .. },
        } = Cli::try_parse_from([
            "ravel",
            "cache",
            "reclaim-legacy",
            "--cache-dir",
            "/var/cache/rv",
            "--apply",
        ])
        .expect("a reclaim-legacy --apply invocation parses")
        .command
        else {
            panic!("expected the cache reclaim-legacy subcommand");
        };
        assert!(apply, "--apply flips to true when given");
    }

    /// `store qualify --list-page-size` takes an operator-supplied count that
    /// the conformance suite turns into writes: it puts `page_size + 2`
    /// objects per listing probe into the bucket. Both ends of the range are
    /// refused at parse time, before the store is built or a single scratch
    /// object is written.
    #[test]
    fn list_page_size_is_range_checked() {
        use super::StoreCommand;

        let Command::Store {
            command: StoreCommand::Qualify { list_page_size },
        } = Cli::try_parse_from(["ravel", "store", "qualify"])
            .expect("a bare qualify invocation parses")
            .command
        else {
            panic!("expected the store qualify subcommand");
        };
        assert_eq!(
            list_page_size,
            ravel_object_store::s3::LIST_PAGE_SIZE,
            "the default is the production S3 page size"
        );

        for out_of_range in ["0", "1000001"] {
            let err = Cli::try_parse_from([
                "ravel",
                "store",
                "qualify",
                "--list-page-size",
                out_of_range,
            ])
            .expect_err("a page size outside 1..=1_000_000 must be refused");
            assert!(
                err.to_string().contains("1..=1000000"),
                "the refusal must name the accepted range, got: {err}"
            );
        }

        let Command::Store {
            command: StoreCommand::Qualify { list_page_size },
        } = Cli::try_parse_from(["ravel", "store", "qualify", "--list-page-size", "1000000"])
            .expect("the upper bound itself parses")
            .command
        else {
            panic!("expected the store qualify subcommand");
        };
        assert_eq!(list_page_size, 1_000_000);
    }

    #[test]
    fn status_mask_names_known_bits() {
        assert_eq!(rspan_status_mask_names(0), "none");
        assert_eq!(rspan_status_mask_names(STATUS_BIT_UNSET), "unset");
        assert_eq!(
            rspan_status_mask_names(STATUS_BIT_UNSET | STATUS_BIT_OK),
            "unset|ok"
        );
        assert_eq!(
            rspan_status_mask_names(STATUS_BIT_OK | STATUS_BIT_ERROR),
            "ok|error"
        );
    }

    /// A bit the flag table does not name must still print, by position, so an
    /// operator can tell "not understood" from "not set". The RSPAN reader
    /// rejects a reserved bit before an object ever reaches the inspector
    /// (docs/span-segment-format.md "SKIP_IDX"), so this defensive rendering is
    /// exercised directly rather than through a crafted object.
    #[test]
    fn status_mask_names_unknown_bit_prints_position() {
        // Bit 5 is reserved and unnamed; bit 5 alone must render as `bit5`.
        let out = rspan_status_mask_names(0b0010_0000);
        assert_eq!(out, "bit5");

        // A known bit mixed with an unnamed one keeps the name and appends the
        // unknown bit's position; neither is dropped.
        let out = rspan_status_mask_names(STATUS_BIT_ERROR | 0b0000_1000);
        assert_eq!(out, "error|bit3");
        assert!(out.contains("bit3"), "unnamed bit position must appear");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod cli_reference_tests {
    //! Drift check for `docs/reference/ravel-cli-flags.md` (ADR-1040 decision
    //! 4). The tables are rendered from `Cli`'s clap definition by the shared
    //! renderer in `ravel-server` and compared to the committed file;
    //! `RAVEL_UPDATE_CLI_REFERENCE=1` rewrites the file instead of asserting.
    //!
    //! The renderer lives in `ravel-server` (already a dev-dependency of this
    //! crate) so the two binaries share one table format rather than
    //! duplicating it.

    use std::env;
    use std::path::{Path, PathBuf};

    use clap::CommandFactory;
    use ravel_server::cli_reference::{
        BEGIN_MARKER, END_MARKER, count_data_rows, render_command_tree_block, splice, user_args,
    };

    use super::Cli;

    const UPDATE_ENV: &str = "RAVEL_UPDATE_CLI_REFERENCE";
    const REGEN: &str = "RAVEL_UPDATE_CLI_REFERENCE=1 cargo test -p ravel-cli";

    /// `docs/reference/ravel-cli-flags.md`, resolved from this crate's manifest
    /// directory so the path is stable regardless of the test's working
    /// directory.
    fn reference_doc() -> PathBuf {
        let manifest = env!("CARGO_MANIFEST_DIR");
        Path::new(manifest)
            .join("..")
            .join("..")
            .join("docs")
            .join("reference")
            .join("ravel-cli-flags.md")
    }

    /// Total user-defined arguments across the whole command tree: the global
    /// flags plus every subcommand's own arguments, counted from the command
    /// definition rather than from rendered text.
    fn total_args(cmd: &clap::Command) -> usize {
        let mut count = user_args(cmd).count();
        for sub in cmd.get_subcommands() {
            if sub.get_name() == "help" {
                continue;
            }
            count += total_args(sub);
        }
        count
    }

    /// Every argument clap reports across the tree becomes exactly one rendered
    /// row, so a generator that walked only the top level (or emitted one row)
    /// would fail here.
    #[test]
    fn tree_has_one_row_per_argument() {
        let cmd = Cli::command();
        let expected = total_args(&cmd);
        let block = render_command_tree_block(&cmd);
        let rows = count_data_rows(&block);
        assert!(
            expected > 50,
            "only {expected} arguments found across the ravel-cli tree, so the \
             definition did not load"
        );
        assert_eq!(
            rows, expected,
            "the generated ravel-cli tables have {rows} rows but the command \
             tree defines {expected} arguments"
        );
    }

    /// The block carries a table for the global flags and a heading per
    /// subcommand, walked rather than hard-coded.
    #[test]
    fn every_subcommand_has_a_heading() {
        let cmd = Cli::command();
        let block = render_command_tree_block(&cmd);
        for sub in cmd.get_subcommands() {
            if sub.get_name() == "help" {
                continue;
            }
            let heading = format!("## {}\n", sub.get_name());
            assert!(
                block.contains(&heading),
                "no heading for subcommand `{}` in the generated block",
                sub.get_name()
            );
        }
    }

    /// The committed reference matches what the current command tree renders.
    #[test]
    fn cli_reference_is_current() {
        let cmd = Cli::command();
        let block = render_command_tree_block(&cmd);

        let path = reference_doc();
        let doc = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let updated = splice(&doc, &block)
            .unwrap_or_else(|| panic!("{} is missing the generated markers", path.display()));

        if env::var(UPDATE_ENV).as_deref() == Ok("1") {
            if updated != doc {
                std::fs::write(&path, &updated)
                    .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
                eprintln!("rewrote {}", path.display());
            }
            return;
        }

        assert_eq!(
            updated, doc,
            "docs/reference/ravel-cli-flags.md is stale. Regenerate it with:\n  \
             {REGEN}\nMarkers: {BEGIN_MARKER} .. {END_MARKER}"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, dead_code)]
#[path = "fake_s3.rs"]
mod fake_s3;

/// `--tenant-kms-config` driven through [`run`], the function `main` calls,
/// for each command that takes it, against a key-epoch record ravel-server's
/// startup already wrote with the file's key: a real run's data writes under
/// the tenant's prefix carry the tenant key and no key epoch is written, and
/// a dry run logs the routing line and writes nothing under that prefix.
/// These pin each call site's `dry_run` argument, which the library tests of
/// `build_tenant_data_store` cannot see.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod tenant_kms_dispatch_tests {
    use std::io::Write;

    use clap::Parser;
    use ravel_object_store::ObjectStoreBackend;
    use ravel_types::{Signal, TenantId};

    use super::fake_s3::seed::{self, NS_PER_HOUR};
    use super::fake_s3::{Echo, FakeS3, SeenPut, spawn};
    use super::{Cli, run_logged, store};

    const TENANT: &str = "acme";
    const TENANT_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/acme-key";
    const HOUR: u32 = 100;

    fn kms_file() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        write!(file, "[tenants]\n{TENANT} = \"{TENANT_KEY}\"\n").expect("write kms file");
        file
    }

    fn store_flags(endpoint: &str) -> Vec<String> {
        [
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            endpoint,
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
            "--tenant-hash-unkeyed",
        ]
        .map(str::to_string)
        .to_vec()
    }

    fn plain_store(endpoint: &str) -> std::sync::Arc<dyn ObjectStoreBackend> {
        let flags = store_flags(endpoint);
        let without_tenancy = &flags[..flags.len() - 1];
        let args = store::StoreArgs::try_parse_from(without_tenancy).expect("store flags parse");
        store::build_store(&args).expect("plain store")
    }

    /// Parse `command` after the global store flags, with the KMS file, and
    /// run it exactly as `main` does, returning what it logged in place of
    /// stderr.
    async fn run_command_logged(
        endpoint: &str,
        file: &tempfile::NamedTempFile,
        command: &[&str],
    ) -> (anyhow::Result<()>, String) {
        let mut argv = store_flags(endpoint);
        argv.extend(command.iter().map(|arg| arg.to_string()));
        argv.push("--tenant-kms-config".to_string());
        argv.push(file.path().display().to_string());
        let mut log = Vec::new();
        let result = run_logged(Cli::try_parse_from(argv).expect("command parses"), &mut log).await;
        (result, String::from_utf8(log).expect("utf-8 log"))
    }

    async fn run_command(
        endpoint: &str,
        file: &tempfile::NamedTempFile,
        command: &[&str],
    ) -> anyhow::Result<()> {
        run_command_logged(endpoint, file, command).await.0
    }

    fn routing_line() -> String {
        format!("tenant-kms: tenant \"{TENANT}\" writes are encrypted under {TENANT_KEY}\n")
    }

    /// The key-epoch record as ravel-server's startup leaves it for the
    /// file's key.
    async fn seed_server_epochs(endpoint: &str) {
        seed::key_epochs(plain_store(endpoint).as_ref(), TENANT, TENANT_KEY).await;
    }

    fn tenant_prefix() -> String {
        format!("t/{}/", TenantId::new(TENANT).hash().to_hex())
    }

    fn tenant_puts_since(fake: &FakeS3, start: usize) -> Vec<SeenPut> {
        let prefix = tenant_prefix();
        fake.puts()
            .into_iter()
            .skip(start)
            .filter(|put| put.key.starts_with(&prefix))
            .collect()
    }

    /// The command wrote no key epoch, and every write under the tenant's
    /// prefix carried the tenant key. Returns those routed keys.
    fn assert_routed(data: &[SeenPut]) -> Vec<String> {
        let enc = format!("{}enc", tenant_prefix());
        assert_eq!(
            data.iter().filter(|put| put.key == enc).count(),
            0,
            "ravel-cli writes no key epoch"
        );
        let unrouted: Vec<(&str, Option<&str>)> = data
            .iter()
            .filter(|put| put.sse_kms_key_id.as_deref() != Some(TENANT_KEY))
            .map(|put| (put.key.as_str(), put.sse_kms_key_id.as_deref()))
            .collect();
        assert!(
            unrouted.is_empty(),
            "every data write must carry {TENANT_KEY}; these did not: {unrouted:?}"
        );
        assert!(!data.is_empty(), "the command wrote tenant data");
        data.iter().map(|put| put.key.clone()).collect()
    }

    fn assert_nothing_written(fake: &FakeS3, start: usize) {
        assert_eq!(
            tenant_puts_since(fake, start),
            Vec::new(),
            "nothing is written under the tenant's prefix, not even enc"
        );
    }

    async fn seeded_logs(endpoint: &str) {
        seed_server_epochs(endpoint).await;
        seed::two_l0_logs(plain_store(endpoint).as_ref(), TENANT, 0, HOUR).await;
    }

    const COMPACT_BUCKET: [&str; 10] = [
        "maintain",
        "compact-bucket",
        "--tenant",
        TENANT,
        "--signal",
        "logs",
        "--shard",
        "0",
        "--hour",
        "100",
    ];
    const COMPACT_TENANT: [&str; 12] = [
        "maintain",
        "compact-tenant",
        "--tenant",
        TENANT,
        "--signal",
        "logs",
        "--shards",
        "1",
        "--from-hour",
        "100",
        "--to-hour",
        "100",
    ];
    const MIGRATE: [&str; 8] = [
        "maintain", "migrate", "--tenant", TENANT, "--signal", "logs", "--shards", "1",
    ];

    /// Non-vacuity: pass `true` for `dry_run` at the compact-bucket call site
    /// in `run_logged` and the routed-write assertion fails: every L1 part
    /// unrouted.
    #[tokio::test]
    async fn compact_bucket_real_run_routes_through_the_tenant_key() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        seeded_logs(&endpoint).await;
        let start = fake.puts().len();
        run_command(&endpoint, &kms_file(), &COMPACT_BUCKET)
            .await
            .expect("compact-bucket runs");
        let routed = assert_routed(&tenant_puts_since(&fake, start));
        assert!(routed.iter().any(|key| key.contains("/l1/")), "{routed:?}");
    }

    /// Non-vacuity: pass `false` for `dry_run` at the compact-bucket call site
    /// and the routed store is built, so the compaction writes its L1 parts.
    #[tokio::test]
    async fn compact_bucket_dry_run_writes_nothing() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        seeded_logs(&endpoint).await;
        let start = fake.puts().len();
        let mut command = COMPACT_BUCKET.to_vec();
        command.push("--dry-run");
        let (result, log) = run_command_logged(&endpoint, &kms_file(), &command).await;
        result.expect("compact-bucket dry run");
        assert_eq!(
            log,
            routing_line(),
            "a dry run logs the real run's routing line"
        );
        assert_nothing_written(&fake, start);
    }

    /// A dry run refuses a differing recorded key with the real run's message.
    ///
    /// Non-vacuity: drop the `check_tenant_kms_records` call from the dry-run
    /// branch of `build_tenant_data_store` and the dry run succeeds.
    #[tokio::test]
    async fn compact_bucket_dry_run_refuses_a_differing_recorded_key() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        let plain = plain_store(&endpoint);
        seed::key_epochs(plain.as_ref(), TENANT, RECORDED_KEY).await;
        seed::two_l0_logs(plain.as_ref(), TENANT, 0, HOUR).await;
        let start = fake.puts().len();
        let mut command = COMPACT_BUCKET.to_vec();
        command.push("--dry-run");
        let err = run_command(&endpoint, &kms_file(), &command)
            .await
            .expect_err("a differing recorded key refuses the dry run");
        assert_eq!(err.to_string(), differing_key_refusal());
        assert_nothing_written(&fake, start);
    }

    #[tokio::test]
    async fn compact_tenant_real_run_routes_through_the_tenant_key() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        seeded_logs(&endpoint).await;
        let start = fake.puts().len();
        run_command(&endpoint, &kms_file(), &COMPACT_TENANT)
            .await
            .expect("compact-tenant runs");
        let routed = assert_routed(&tenant_puts_since(&fake, start));
        assert!(routed.iter().any(|key| key.contains("/l1/")), "{routed:?}");
    }

    #[tokio::test]
    async fn compact_tenant_dry_run_writes_nothing() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        seeded_logs(&endpoint).await;
        let start = fake.puts().len();
        let mut command = COMPACT_TENANT.to_vec();
        command.push("--dry-run");
        let (result, log) = run_command_logged(&endpoint, &kms_file(), &command).await;
        result.expect("compact-tenant dry run");
        assert_eq!(log, routing_line());
        assert_nothing_written(&fake, start);
    }

    async fn provisioned_logs(endpoint: &str) {
        let plain = plain_store(endpoint);
        ravel_catalog::validate_or_adopt(
            plain.as_ref(),
            &TenantId::new(TENANT).hash(),
            Signal::Logs,
            1,
            0,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
        seed::key_epochs(plain.as_ref(), TENANT, TENANT_KEY).await;
        seed::two_l0_logs(plain.as_ref(), TENANT, 0, HOUR).await;
    }

    /// A walk with nothing below the target still raises the floor, which
    /// rewrites the provisioning record: a routed data write.
    #[tokio::test]
    async fn migrate_real_run_routes_through_the_tenant_key() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        provisioned_logs(&endpoint).await;
        let start = fake.puts().len();
        run_command(&endpoint, &kms_file(), &MIGRATE)
            .await
            .expect("migrate runs");
        let routed = assert_routed(&tenant_puts_since(&fake, start));
        assert!(
            routed.iter().any(|key| key.ends_with("/prov")),
            "{routed:?}"
        );
    }

    #[tokio::test]
    async fn migrate_dry_run_writes_nothing() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        provisioned_logs(&endpoint).await;
        let start = fake.puts().len();
        let mut command = MIGRATE.to_vec();
        command.push("--dry-run");
        let (result, log) = run_command_logged(&endpoint, &kms_file(), &command).await;
        result.expect("migrate dry run");
        assert_eq!(log, routing_line());
        assert_nothing_written(&fake, start);
    }

    /// `catalog fold` has no dry run; its call site passes `false`.
    #[tokio::test]
    async fn catalog_fold_routes_through_the_tenant_key() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        let now = ravel_cli::now_ns().expect("wall clock");
        seed::metrics_l0(
            plain_store(&endpoint).as_ref(),
            TENANT,
            0,
            1,
            now - 3 * NS_PER_HOUR,
        )
        .await;
        seed_server_epochs(&endpoint).await;
        let start = fake.puts().len();
        run_command(
            &endpoint,
            &kms_file(),
            &[
                "catalog", "fold", "--tenant", TENANT, "--shards", "1", "--signal", "metrics",
            ],
        )
        .await
        .expect("fold runs");
        let routed = assert_routed(&tenant_puts_since(&fake, start));
        let head = format!("{}catalog/m/HEAD", tenant_prefix());
        assert!(routed.contains(&head), "{routed:?}");
    }

    const RECORDED_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/acme-old-key";

    fn differing_key_refusal() -> String {
        format!(
            "failed to configure per-tenant SSE-KMS routing (--tenant-kms-config): tenant \
             \"{TENANT}\" has key \"{RECORDED_KEY}\" recorded as its current key epoch at \
             t/{}/enc, but --tenant-kms-config names \"{TENANT_KEY}\": a key change is recorded \
             by ravel-server at startup, never by this command. Refusing before any write; \
             start ravel-server with the new key first, or run this command with the file the \
             servers run with",
            TenantId::new(TENANT).hash().to_hex()
        )
    }

    /// The refusal reaches the operator as the command's error, before the
    /// command writes anything under the tenant's prefix.
    #[tokio::test]
    async fn a_differing_recorded_key_refuses_the_command() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        let plain = plain_store(&endpoint);
        seed::key_epochs(plain.as_ref(), TENANT, RECORDED_KEY).await;
        seed::two_l0_logs(plain.as_ref(), TENANT, 0, HOUR).await;
        let start = fake.puts().len();

        let err = run_command(&endpoint, &kms_file(), &COMPACT_BUCKET)
            .await
            .expect_err("a differing recorded key refuses");
        assert_eq!(err.to_string(), differing_key_refusal());
        assert_nothing_written(&fake, start);
    }

    /// A tenant with no key-epoch record refuses the command: ravel-cli
    /// records no key, so the operator starts ravel-server with the file
    /// first. Nothing is written under the tenant's prefix, enc included.
    ///
    /// Non-vacuity: make `epoch_action` return `Ok(EpochAction::Bootstrap)`
    /// for an absent record under `KeyChangePolicy::Refuse` and the command
    /// runs, writing epochs 0 and 1 and its L1 parts.
    #[tokio::test]
    async fn an_absent_key_epoch_record_refuses_the_command() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        seed::two_l0_logs(plain_store(&endpoint).as_ref(), TENANT, 0, HOUR).await;
        let start = fake.puts().len();

        let err = run_command(&endpoint, &kms_file(), &COMPACT_BUCKET)
            .await
            .expect_err("an absent record refuses");
        assert_eq!(
            err.to_string(),
            format!(
                "failed to configure per-tenant SSE-KMS routing (--tenant-kms-config): tenant \
                 \"{TENANT}\" has no key-epoch record at t/{}/enc, but --tenant-kms-config \
                 names \"{TENANT_KEY}\" for it: a tenant's key epochs are recorded by \
                 ravel-server at startup, never by this command. Refusing before any write; \
                 start ravel-server with this --tenant-kms-config file first, then rerun this \
                 command",
                TenantId::new(TENANT).hash().to_hex()
            )
        );
        assert_nothing_written(&fake, start);
    }
}

/// `parquet repair --stray` driven through [`run`], the function `main`
/// calls, so the dispatch's handling of `--include-reserved-names` is what
/// is pinned, not only the parsed flag.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod parquet_repair_dispatch_tests {
    use bytes::Bytes;
    use clap::Parser;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_types::TenantId;

    use super::fake_s3::{Echo, spawn};
    use super::{Cli, run_logged, store};

    const TENANT: &str = "acme";

    fn argv(endpoint: &str, command: &[&str]) -> Vec<String> {
        [
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            endpoint,
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
            "--tenant-hash-unkeyed",
        ]
        .iter()
        .chain(command)
        .map(|arg| arg.to_string())
        .collect()
    }

    /// A well-formed manifest key of `acme` under `l0`, a name reserved
    /// after tables could be created, written straight to the bucket.
    async fn reserved_key(endpoint: &str) -> String {
        let key = format!(
            "t/{}/pq/t/l0/v/00000000000000000001.pqm",
            TenantId::new(TENANT).hash().to_hex()
        );
        let args = store::StoreArgs::try_parse_from(&argv(endpoint, &[])[..11])
            .expect("store flags parse");
        store::build_store(&args)
            .expect("store")
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
        key
    }

    async fn run_repair(endpoint: &str, tail: &[&str]) -> anyhow::Result<()> {
        let mut command = vec![
            "parquet", "repair", "--tenant", TENANT, "--stray", "--delete",
        ];
        command.extend(tail);
        let cli = Cli::try_parse_from(argv(endpoint, &command)).expect("command parses");
        run_logged(cli, &mut Vec::new()).await
    }

    /// Without `--include-reserved-names` the reserved-name key is skipped
    /// and the run fails naming it; with it, the dispatch hands `true` to the
    /// repair and the key is deleted.
    ///
    /// Non-vacuity: pass `false` for `include_reserved_names` at the
    /// `repair_stray` call in `run_logged` and the second run sends no
    /// delete and fails naming the key.
    #[tokio::test]
    async fn include_reserved_names_reaches_the_stray_repair() {
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        let key = reserved_key(&endpoint).await;

        let err = run_repair(&endpoint, &[])
            .await
            .expect_err("the reserved-name key is skipped");
        assert_eq!(
            err.to_string(),
            format!("1 key(s) under no valid table name still listed after --delete: {key:?}")
        );
        assert_eq!(fake.deletes(), Vec::<String>::new());
        assert!(fake.has_object(&key));

        run_repair(&endpoint, &["--include-reserved-names"])
            .await
            .expect("the reserved-name key is deleted");
        assert_eq!(fake.deletes(), vec![key.clone()]);
        assert!(!fake.has_object(&key));
    }
}
