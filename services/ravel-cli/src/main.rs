//! ravel-cli: inspect segments, decode commit records, list catalog entries.

use clap::{Parser, Subcommand};
use ravel_cli::maintain::SignalArg;
use ravel_cli::{catalog, maintain, now_ns, store};
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

#[derive(Debug, Parser)]
#[command(name = "ravel-cli", about = "Ravel dev inspection CLI")]
struct Cli {
    #[command(flatten)]
    store: store::StoreArgs,

    #[command(subcommand)]
    command: Command,
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
    /// Run and inspect maintenance: compaction, sweep, retention, version audit
    /// (docs/compaction-retention-plan.md P8).
    Maintain {
        #[command(subcommand)]
        command: MaintainCommand,
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
    /// One-shot catalog fold for a tenant (docs/metric-index-plan.md section 4).
    Fold {
        #[arg(long)]
        tenant: String,
        #[arg(long, default_value_t = 4)]
        shards: u32,
    },
    /// Decode and print HEAD and every referenced snapshot part.
    Inspect {
        #[arg(long)]
        tenant: String,
    },
    /// Re-list sealed commit records and diff against the snapshot; exits
    /// nonzero if the snapshot is missing or mismatches sealed history.
    Verify {
        #[arg(long)]
        tenant: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
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
        Command::Catalog {
            command:
                CatalogCommand::List {
                    tenant,
                    hours,
                    shards,
                },
        } => catalog_list(&cli.store, &tenant, hours, shards).await,
        Command::Catalog {
            command: CatalogCommand::Fold { tenant, shards },
        } => catalog::fold(store::build_store(&cli.store)?, &tenant, shards).await,
        Command::Catalog {
            command: CatalogCommand::Inspect { tenant },
        } => catalog::inspect(store::build_store(&cli.store)?, &tenant).await,
        Command::Catalog {
            command: CatalogCommand::Verify { tenant },
        } => catalog::verify(store::build_store(&cli.store)?, &tenant).await,
        Command::Maintain {
            command:
                MaintainCommand::CompactBucket {
                    tenant,
                    signal,
                    shard,
                    hour,
                    dry_run,
                },
        } => {
            maintain::compact(
                store::build_store(&cli.store)?,
                &tenant,
                signal,
                shard,
                hour,
                dry_run,
            )
            .await
        }
        Command::Maintain {
            command:
                MaintainCommand::Sweep {
                    tenant,
                    signal,
                    shard,
                    dry_run,
                },
        } => {
            maintain::sweep(
                store::build_store(&cli.store)?,
                &tenant,
                signal,
                shard,
                dry_run,
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
        _ => "UNKNOWN",
    }
}

/// Human-readable name for a `ravel_segment::ValueKind`, matching the wire
/// names from SERIES_META column 10 (docs/rseg-v3-plan.md section 3.4:
/// `0 = VAL_SCALAR, 1 = HIST_SPANS`), not `ValueKind`'s Rust variant names.
fn value_kind_name(kind: ravel_segment::ValueKind) -> &'static str {
    match kind {
        ravel_segment::ValueKind::Scalar => "VAL_SCALAR",
        ravel_segment::ValueKind::Histogram => "HIST_SPANS",
    }
}

/// Human-readable name for a `ravel_segment::ResetHint`, matching
/// Prometheus's four reset-hint states (docs/rseg-v3-plan.md section 3.5).
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

/// Prints one decoded histogram record (docs/rseg-v3-plan.md section 3.5):
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

    segment_inspect_v5(bytes, footer, limits)
}

/// v5 catalog decode and print (docs/segment-format.md). ADR-0027 leaves v5
/// the only version; the run-major catalog is decoded over the whole object
/// (folding the chunked SERIES_META, or the whole SERIES_META below the
/// sparse threshold), and each series prints its per-run provenance and page
/// ranges. `schema_count` is derived from the decoded label sets (distinct
/// name-only tuples, first-appearance order), the same "(derived)" caveat the
/// pre-v5 inspector carried. Histogram runs decode their HIST/TS pages and
/// print every record's full field detail.
fn segment_inspect_v5(
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
fn rlog_print_stats(stats: &[NumStat]) {
    for st in stats {
        println!(
            "    stat column_id={} type={} min_bits={} max_bits={} null_count={} has_nan={}",
            st.column_id,
            rlog_field_type_name(st.ty),
            st.min_bits,
            st.max_bits,
            st.null_count,
            st.has_nan,
        );
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

/// Human-readable name for a known RSPAN section kind
/// (docs/span-segment-format.md).
fn rspan_section_kind_name(kind: u32) -> &'static str {
    match kind {
        ravel_rspan::footer::kind::BLOCKS => "BLOCKS",
        ravel_rspan::footer::kind::SKIP_IDX => "SKIP_IDX",
        _ => "UNKNOWN",
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
/// and summary, the section table, and the interval-aware skip index (one line
/// per block). Every decode is the reader's own path, so a corrupt SKIP_IDX or a
/// section crc mismatch surfaces as a typed error with a non-zero exit, never a
/// panic.
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
             trace_id_range=[{}, {}] start_ts_min={} end_ts_max={}",
            entry.block_offset,
            entry.block_len,
            entry.block_crc32c,
            entry.record_count,
            hex::encode(entry.min_trace_id),
            hex::encode(entry.max_trace_id),
            entry.min_start_ts,
            entry.max_end_ts,
        );
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
    let catalog = ravel_catalog::Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?;

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
