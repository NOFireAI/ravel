//! ravel-cli: inspect segments, decode commit records, list catalog entries.

use clap::{Parser, Subcommand};
use ravel_cli::maintain::SignalArg;
use ravel_cli::{
    catalog, hold, idem, maintain, now_ns, parse_max_flush_lifetime_ns, store, tenancy,
    tenant_token,
};
use ravel_logseg::block::NumStat;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, COMP_NONE, COMP_ZSTD, kind};
use ravel_logseg::record::FieldType;
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{RlogConfig, read_section};
use ravel_proto::segment::v1::Footer;
use ravel_types::{Signal, TenantId, TimeRange};

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
        | Command::Load { .. } => true,
        // `commit reconstruct` computes a `t/<tenant_hash>/` prefix from its
        // `--tenant`, so it needs the bucket's scheme resolved first; the
        // other `commit` variants take an explicit key/path and do not.
        Command::Commit { command } => matches!(command, CommitCommand::Reconstruct { .. }),
        Command::Segment { .. }
        | Command::Rlog { .. }
        | Command::Rspan { .. }
        | Command::Store { .. }
        | Command::Idem { .. }
        | Command::Tenancy { .. }
        // sys/gc is a bucket-root object, not under any tenant prefix, so
        // gc-config never hashes a tenant. sys/auth (ADR-0072 decision 4) is
        // the same shape: deployment-wide, at the bucket root, never under a
        // tenant prefix, so `tenant token` never hashes a tenant either.
        | Command::GcConfig { .. }
        | Command::Tenant { .. } => false,
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
    /// Show or set a tenant's durable declared typed attribute columns for the
    /// `logs` SQL table (ADR-0090 decision 1), in
    /// `TenantConfig.typed_attr_columns` at `t/<tenant_hash>/config`. A
    /// query-serving process picks a change up within its declared-column
    /// staleness horizon; no restart is needed.
    TypedAttrColumn {
        #[command(subcommand)]
        command: TypedAttrColumnCommand,
    },
    /// Manage the durable deployment-wide bearer-token map `sys/auth`
    /// (ADR-0072 decision 4): the writer of `sys/auth`.
    Tenant {
        #[command(subcommand)]
        command: TenantCommand,
    },
    /// Bulk-import a Parquet file into the logs signal (ADR-0089).
    ///
    /// Writes directly to the log ingest router in-process. NOTE: the
    /// per-tenant AdmissionController (active-stream cap, stream-creation rate,
    /// byte rate) that guards the HTTP ingest path is BYPASSED by construction
    /// on this path. The future-skew and length caps are enforced identically
    /// to OTLP; the past-event-time lag check is deliberately NOT enforced, so
    /// historical timestamps are admitted (they bucket by load time, so query
    /// with a window that reaches now). A per-record attribute cap of 1024
    /// applies (relaxed from OTLP's 128). A row that fails a kept check is
    /// rejected fail-fast: the run stops at the first bad row and exits
    /// nonzero. There is NO resumability or deduplication: re-running after a
    /// failure re-ingests the whole file from the start, and retention is
    /// measured from load time, not the records' event times.
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
        /// Configured shard count. Validated against (or, for a fresh signal,
        /// written to) the durable provisioning record, exactly as the server
        /// does at first touch; the router resolves the active generation from
        /// that record. Defaults to the server's default of 4.
        #[arg(long, default_value_t = 4)]
        shards: u32,
        /// Rows per Strict flush. One flush is one RLOG object per involved
        /// shard, so on a large load this is the lever that controls how many
        /// RLOG objects the load leaves behind (a first-order query-cost
        /// variable). Omit for a size-aware default derived from the input's own
        /// row count and uncompressed byte size (`resolve_batch_rows`): an input
        /// under a million rows keeps `DEFAULT_BATCH_ROWS` (10000) and loads
        /// exactly as it did before, while a bulk import is sized up to
        /// `--shards` x the RLOG block target (8192 rows), so every shard's
        /// average per-batch slice fills one block instead of writing a short
        /// one, capped by what the resident batch working set can hold at the
        /// input's average row width. The value actually used is printed in the
        /// load's effective-configuration block. An explicit value is used
        /// verbatim and must be at least 1; 0 is rejected.
        #[arg(long)]
        batch_rows: Option<usize>,
        /// Number of parallel stride read cursors over the Parquet file's row
        /// groups (issue #560). A file sorted by a resource-attribute column
        /// (e.g. ClickBench's `hits.parquet`, sorted by `CounterID`) puts one
        /// value's rows in one contiguous run, so a single sequential reader
        /// fills each `--batch-rows` batch with just that one value: one
        /// `shard_for_log` hash, one shard, no spread across `--shards`. K
        /// cursors each read a disjoint, near-even, far-apart partition of the
        /// file's row groups, and each batch is assembled from a contiguous
        /// run out of every live cursor, so one batch's rows span K different
        /// regions of the file instead of one. Omit to default to `--shards`,
        /// clamped down to the row-group count and floored at 1, so a batch can
        /// spread across every shard without the operator matching the two
        /// flags by hand; an explicit value is clamped to `[1, row-group
        /// count]`. `1` is a sequential read. `0` is rejected.
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
        /// batch resident until its ack, so the live working set scales by
        /// roughly the depth (see docs/guides/clickbench.md for how this stacks
        /// with the `--batch-rows` x `--shards` product). The reported
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
        /// Bytes a shard's buffer accumulates before it flushes as one RLOG
        /// object (issue #801). At the default `1` every batch flushes as its
        /// own object the moment it is written: one object per involved shard
        /// per batch, `--batch-rows` sets its size, and no buffer lingers. A
        /// larger value lets a shard accumulate ENCODED records across several
        /// batches until the target is reached, so objects grow without any
        /// more Arrow batches being held in memory -- unlike raising
        /// `--batch-rows`, whose memory cost is linear because each batch is
        /// buffered whole.
        ///
        /// The trade is ack timing, not durability. A Strict write's ack is
        /// still sent only after its records' object and commit record are
        /// published, so an ack always means durable. But above `1` the flush
        /// that answers a batch's ack may be triggered by a LATER batch, so
        /// that ack now waits for one; a buffer that never reaches the target
        /// waits for the router's wall-clock age trigger instead
        /// (`max_flush_delay`, 2s). Set `--pipeline-depth` to at least the
        /// number of batches that accumulate into one flush, or every flush
        /// waits out that timer. `0` is rejected.
        #[arg(long, value_name = "BYTES", default_value_t = ravel_cli::load::DEFAULT_TARGET_BYTES)]
        target_bytes: usize,
    },
}

#[derive(Debug, Subcommand)]
enum TenantCommand {
    /// Manage bearer tokens in `sys/auth`.
    Token {
        #[command(subcommand)]
        command: TenantTokenCommand,
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
        /// `[[resource_attribute]]` entry becomes a declared column of the
        /// same-named type. `f64`-typed entries are skipped with a per-key
        /// warning on stderr (there is no `f64` declared column type); the rest
        /// are written through the same CAS whole-list replace. Mutually
        /// exclusive with positional `KEY:TYPE` specs.
        #[arg(long, value_name = "TOML")]
        from_mapping: Option<std::path::PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum GcConfigCommand {
    /// Print the durable `sys/gc` values (protection horizon, grace, max query
    /// duration, max flush lifetime), or report that the bucket is not yet
    /// bootstrapped.
    Show {},
    /// Write a full new `sys/gc`, enforcing `protection_horizon >=
    /// max_query_duration + grace + clock_skew_allowance` at write time and
    /// swapping the durable object with `CasVersion`. All durations are
    /// humantime strings (e.g. `25h5m`).
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
        /// Longest a flush may stay open (e.g. `1h`).
        #[arg(long, value_name = "DURATION")]
        max_flush_lifetime: String,
        /// Cross-host clock-skew allowance the horizon must cover (e.g. `5m`).
        /// The constraint input that closes S1-02; must match the sweepers'
        /// `clock_skew_allowance`. Not stored in `sys/gc`. Defaults to 5m when
        /// omitted.
        #[arg(long, value_name = "DURATION")]
        clock_skew_allowance: Option<String>,
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
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Run the conformance suite against the configured backend and, on a
    /// pass, record the outcome at `sys/qualification`.
    Qualify {},
}

#[derive(Debug, Subcommand)]
enum SegmentCommand {
    Inspect {
        /// Local file path or object store key.
        path: String,
    },
}

#[derive(Debug, Subcommand)]
enum RlogCommand {
    Inspect {
        /// Local file path or object store key.
        path: String,
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
    /// Run one compaction pass over a single sealed bucket.
    CompactBucket {
        #[arg(long)]
        tenant: String,
        #[arg(long, value_enum)]
        signal: SignalArg,
        #[arg(long)]
        shard: u32,
        #[arg(long)]
        hour: u32,
        /// Compute the plan and report it, but write no L1 parts or record.
        #[arg(long)]
        dry_run: bool,
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
    },
    /// Compact every sealed bucket of a whole tenant signal: walk each shard's
    /// ingest hours and run the same per-bucket compaction `compact-bucket`
    /// runs, so an operator no longer has to guess the hour numbers or write a
    /// per-(shard, hour) shell loop.
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
        /// Compute each bucket's plan and report it, but write no L1 parts or
        /// records.
        #[arg(long)]
        dry_run: bool,
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
    },
    /// Run one sweep pass (orphan GC, superseded, unreferenced parts) over a shard.
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
    /// Audit live on-object format versions for a tenant (both signals).
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
    let cli = Cli::parse();

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
        let scheme = tenancy::resolve_scheme(store.as_ref(), configured).await?;
        ravel_types::install_tenant_hash_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("tenant-hash scheme was already installed"))?;
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
                },
        } => catalog::fold(
            store::build_store(&cli.store)?,
            &tenant,
            shards,
            signal,
            max_flush_lifetime,
            now_ns()?,
        )
        .await
        .map(|_report| ()),
        Command::Catalog {
            command: CatalogCommand::Inspect { tenant, signal },
        } => catalog::inspect(store::build_store(&cli.store)?, &tenant, signal).await,
        Command::Catalog {
            command: CatalogCommand::Verify { tenant, signal },
        } => catalog::verify(store::build_store(&cli.store)?, &tenant, signal).await,
        Command::Maintain {
            command:
                MaintainCommand::CompactBucket {
                    tenant,
                    signal,
                    shard,
                    hour,
                    dry_run,
                    max_flush_lifetime,
                },
        } => {
            maintain::compact(
                store::build_store(&cli.store)?,
                &tenant,
                signal,
                shard,
                hour,
                dry_run,
                max_flush_lifetime,
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
                    max_flush_lifetime,
                },
        } => maintain::compact_tenant(
            store::build_store(&cli.store)?,
            &tenant,
            signal,
            shards,
            from_hour,
            to_hour,
            dry_run,
            max_flush_lifetime,
            now_ns()?,
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
                &tenant,
                signal,
                shard,
                hour,
            )
            .await
        }
        Command::Maintain {
            command: MaintainCommand::AuditVersions { tenant, shards },
        } => maintain::audit_versions(store::build_store(&cli.store)?, &tenant, shards).await,
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
                },
        } => {
            maintain::migrate(
                store::build_store(&cli.store)?,
                &tenant,
                signal,
                shards,
                target_version,
                family,
                budget_records,
            )
            .await
        }
        Command::Store {
            command: StoreCommand::Qualify {},
        } => {
            let run_id = uuid::Uuid::new_v4();
            ravel_cli::qualify::qualify(
                store::build_store(&cli.store)?,
                cli.store.backend_identity(),
                &run_id.to_string(),
            )
            .await
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
                },
        } => {
            ravel_cli::gc_config::set(
                store::build_store(&cli.store)?,
                &protection_horizon,
                &grace,
                &max_query_duration,
                &max_flush_lifetime,
                clock_skew_allowance.as_deref(),
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
        Command::Load {
            parquet,
            tenant,
            mapping,
            shards,
            batch_rows,
            read_cursors,
            pipeline_depth,
            max_inflight_flushes,
            decode_queue_batches,
            target_bytes,
        } => {
            let profile = ravel_cli::cli_profiling::ProfileSession::from_env("ravel-cli-load");
            let result = ravel_cli::load::run(
                store::build_store(&cli.store)?,
                &parquet,
                &tenant,
                &mapping,
                shards,
                batch_rows,
                read_cursors,
                pipeline_depth,
                max_inflight_flushes,
                decode_queue_batches,
                target_bytes,
                now_ns()?,
            )
            .await;
            profile.finish();
            result
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
/// and summary, the section table, the level-0 skip index (one line per block
/// plus its numeric column stats), the stream directory, and the field
/// directory. Every decode is the reader's own path, so a corrupt SKIP_IDX or a
/// section crc mismatch surfaces as a typed error with a non-zero exit, never a
/// panic.
fn rlog_inspect(bytes: &[u8]) -> anyhow::Result<()> {
    let footer = footer::open(bytes)
        .map_err(|err| anyhow::anyhow!("failed to parse rlog segment: {err}"))?;

    println!("total_size: {}", bytes.len());
    println!("version: {}", footer::VERSION);
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

    Ok(())
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

    let tenant_hash = TenantId::new(tenant).hash();
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use clap::Parser;

    use super::rspan_status_mask_names;
    use super::{Cli, Command};
    use ravel_rspan::skip_index::{STATUS_BIT_ERROR, STATUS_BIT_OK, STATUS_BIT_UNSET};

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

    /// `--batch-rows` and `--read-cursors` must reach the loader as `None` when
    /// the operator omits them, because that is what selects the size-aware and
    /// shard-sized defaults (issue #680). A `default_value_t` on either flag
    /// would make every run look explicit and pin the value at the flag layer,
    /// where the input's row count and row-group count are not yet known.
    ///
    /// Non-vacuity (prove-the-test): the pre-change tree had
    /// `#[arg(long, default_value_t = ravel_cli::load::DEFAULT_BATCH_ROWS)]
    /// batch_rows: usize`, which does not compile against `assert_eq!(...,
    /// None)` at all; restoring it as `Option<usize>` with a `default_value_t`
    /// fails this on `Some(10000)` against `None`.
    #[test]
    fn load_input_sized_flags_default_to_none() {
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
        .expect("a load invocation with no sizing flags parses");

        let Command::Load {
            batch_rows,
            read_cursors,
            ..
        } = cli.command
        else {
            panic!("expected the load subcommand");
        };

        assert_eq!(
            batch_rows, None,
            "--batch-rows must reach the loader unset, so it derives a size-aware default"
        );
        assert_eq!(
            read_cursors, None,
            "--read-cursors must reach the loader unset, so it derives one cursor per shard"
        );
    }

    /// An explicit `--batch-rows` still parses through to the loader verbatim.
    #[test]
    fn load_batch_rows_flag_is_passed_through() {
        let cli = Cli::try_parse_from([
            "ravel",
            "load",
            "--parquet",
            "hits.parquet",
            "--tenant",
            "acme",
            "--mapping",
            "hits.toml",
            "--batch-rows",
            "40000",
        ])
        .expect("a load invocation with --batch-rows parses");

        let Command::Load { batch_rows, .. } = cli.command else {
            panic!("expected the load subcommand");
        };
        assert_eq!(batch_rows, Some(40_000));
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
