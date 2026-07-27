//! ravel-cli: inspect segments, decode commit records, list catalog entries.

mod store;

use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use ravel_proto::segment::v1::Footer;
use ravel_types::{Signal, TenantId, TimeRange};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

// Frozen wire values from proto/ravel/segment.proto's `SectionKind` enum.
const SECTION_KIND_LABEL_DICT: u32 = 1;
const SECTION_KIND_SERIES_TABLE: u32 = 2;
const SECTION_KIND_TS_PAGES: u32 = 3;
const SECTION_KIND_VAL_PAGES: u32 = 4;
// v2-only kinds (ADR-0014, docs/segment-format.md "RSEG v2 amendment").
const SECTION_KIND_SERIES_IDS: u32 = 5;
const SECTION_KIND_SERIES_META: u32 = 6;

// Frozen trailer `version` value for RSEG v2 (ADR-0014,
// docs/segment-format.md "RSEG v2 amendment").
const TRAILER_VERSION_V2: u16 = 2;

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
}

#[derive(Debug, Subcommand)]
enum SegmentCommand {
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
}

fn now_ns() -> anyhow::Result<i64> {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH)?;
    i64::try_from(dur.as_nanos()).map_err(|_| anyhow::anyhow!("system clock too far in the future"))
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
        Command::Commit {
            command: CommitCommand::Decode { key },
        } => {
            let bytes = store::read_bytes(&cli.store, &key).await?;
            commit_decode(&bytes)
        }
        Command::Catalog {
            command:
                CatalogCommand::List {
                    tenant,
                    hours,
                    shards,
                },
        } => catalog_list(&cli.store, &tenant, hours, shards).await,
    }
}

fn section_bytes<'a>(bytes: &'a [u8], footer: &Footer, kind: u32) -> anyhow::Result<&'a [u8]> {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .ok_or_else(|| anyhow::anyhow!("segment footer is missing section kind {kind}"))?;
    let start = usize::try_from(section.offset)?;
    let end = start
        .checked_add(usize::try_from(section.len)?)
        .ok_or_else(|| anyhow::anyhow!("section {kind} range overflows"))?;
    bytes
        .get(start..end)
        .ok_or_else(|| anyhow::anyhow!("section {kind} range is out of bounds"))
}

/// Human-readable name for a known section kind. Only used for v2 objects'
/// `sections:` listing (docs/segment-format.md "RSEG v2 amendment" section
/// kinds table): v1 objects' `sections:` output stays exactly as it was
/// before v2 support was added (numeric `kind=` only), so this doesn't
/// touch v1's printed format. Falls back to "UNKNOWN" for any kind not in
/// the frozen set above, matching how readers must skip unknown kinds
/// rather than reject them.
fn section_kind_name(kind: u32) -> &'static str {
    match kind {
        SECTION_KIND_LABEL_DICT => "LABEL_DICT",
        SECTION_KIND_SERIES_TABLE => "SERIES_TABLE",
        SECTION_KIND_TS_PAGES => "TS_PAGES",
        SECTION_KIND_VAL_PAGES => "VAL_PAGES",
        SECTION_KIND_SERIES_IDS => "SERIES_IDS",
        SECTION_KIND_SERIES_META => "SERIES_META",
        _ => "UNKNOWN",
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
    println!("sections:");
    for section in &footer.sections {
        if location.version == TRAILER_VERSION_V2 {
            println!(
                "  kind={} name={} offset={} len={} uncompressed_len={} comp={:?}",
                section.kind,
                section_kind_name(section.kind),
                section.offset,
                section.len,
                section.uncompressed_len,
                section.comp
            );
        } else {
            println!(
                "  kind={} offset={} len={} uncompressed_len={} comp={:?}",
                section.kind, section.offset, section.len, section.uncompressed_len, section.comp
            );
        }
    }

    if location.version == TRAILER_VERSION_V2 {
        segment_inspect_v2(bytes, footer, limits)
    } else {
        segment_inspect_v1(bytes, footer, limits)
    }
}

/// v1 catalog decode and print. Unchanged from the pre-v2-support
/// implementation (only relocated into its own function so `segment_inspect`
/// could add a version dispatch above it); this is the code the v1 golden
/// inspect test pins.
fn segment_inspect_v1(
    bytes: &[u8],
    footer: &Footer,
    limits: ravel_segment::ReaderLimits,
) -> anyhow::Result<()> {
    let label_dict_bytes = section_bytes(bytes, footer, SECTION_KIND_LABEL_DICT)?;
    let series_table_bytes = section_bytes(bytes, footer, SECTION_KIND_SERIES_TABLE)?;
    let entries =
        ravel_segment::decode_catalog(footer, label_dict_bytes, series_table_bytes, limits)
            .map_err(|err| anyhow::anyhow!("failed to decode series catalog: {err}"))?;
    println!("series_count (decoded): {}", entries.len());

    Ok(())
}

/// v2 catalog decode and print (docs/segment-format.md "RSEG v2 amendment").
/// Prints `schema_count` and, for each schema, its label names; and one row
/// per series with the same information content as v1's `SeriesEntry`
/// fields (id, labels, sample count, event-ts bounds), plus the absolute
/// reconstructed page ranges `plan_ranges` computes and a flag for any
/// nonzero alignment gap.
///
/// `schema_count` here is *derived* from the decoded per-series label sets
/// (distinct name-only tuples, first-appearance order in series-id order),
/// not read from SERIES_META's on-disk schema dictionary directly:
/// `decode_catalog_v2`'s public return type is `Vec<SeriesEntry>`, the same
/// shape v1's decode produces, and the schema dictionary itself
/// (`SchemaMetaV2`, `parse_schema_list_v2`) is a private implementation
/// detail of `ravel-segment` not surfaced through any public function. For
/// every object `SegmentWriter::write_v2` produces, this derived count
/// equals the on-disk `schema_count` (every schema it writes is referenced
/// by at least one series). It would *undercount* a hand-crafted v2 object
/// containing a schema with no referencing series -- labeled "(derived)"
/// below specifically so that gap is visible rather than presented as if
/// this tool read the authoritative on-disk value.
fn segment_inspect_v2(
    bytes: &[u8],
    footer: &Footer,
    limits: ravel_segment::ReaderLimits,
) -> anyhow::Result<()> {
    let label_dict_bytes = section_bytes(bytes, footer, SECTION_KIND_LABEL_DICT)?;
    let series_ids_bytes = section_bytes(bytes, footer, SECTION_KIND_SERIES_IDS)?;
    let series_meta_bytes = section_bytes(bytes, footer, SECTION_KIND_SERIES_META)?;
    let entries = ravel_segment::decode_catalog_v2(
        footer,
        label_dict_bytes,
        series_ids_bytes,
        series_meta_bytes,
        limits,
    )
    .map_err(|err| anyhow::anyhow!("failed to decode series catalog: {err}"))?;

    let mut schemas: Vec<Vec<String>> = Vec::new();
    for entry in &entries {
        let names: Vec<String> = entry.labels.iter().map(|l| l.name.clone()).collect();
        if !schemas.contains(&names) {
            schemas.push(names);
        }
    }
    println!("schema_count (derived): {}", schemas.len());
    for (i, schema) in schemas.iter().enumerate() {
        println!("  schema[{i}]: {}", schema.join(","));
    }

    println!("series_count (decoded): {}", entries.len());

    let selected: Vec<&ravel_segment::SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(footer, &selected)
        .map_err(|err| anyhow::anyhow!("failed to plan page ranges: {err}"))?;

    println!("series:");
    // Reconstructs each entry's alignment gap the same way
    // `reconstruct_ranges_v2` produced its section-relative offset in the
    // first place (docs/segment-format.md v2 amendment: `offset_i =
    // end_{i-1} + gap_i`), tracked independently for TS_PAGES and
    // VAL_PAGES. Relies on `entries` (and therefore `ranges`, built from it
    // in the same order) being in the same order `decode_catalog_v2`
    // returned them: ascending series id, matching SERIES_META's on-disk
    // row order.
    let mut ts_end = 0u64;
    let mut val_end = 0u64;
    for (entry, range) in entries.iter().zip(&ranges) {
        let ts_gap = entry
            .ts_page
            .0
            .checked_sub(ts_end)
            .ok_or_else(|| anyhow::anyhow!("ts_page offset precedes running end"))?;
        let val_gap = entry
            .val_page
            .0
            .checked_sub(val_end)
            .ok_or_else(|| anyhow::anyhow!("val_page offset precedes running end"))?;
        ts_end = entry
            .ts_page
            .0
            .checked_add(entry.ts_page.1)
            .ok_or_else(|| anyhow::anyhow!("ts_page range overflows u64"))?;
        val_end = entry
            .val_page
            .0
            .checked_add(entry.val_page.1)
            .ok_or_else(|| anyhow::anyhow!("val_page range overflows u64"))?;

        let labels_str = entry
            .labels
            .iter()
            .map(|l| format!("{}={}", l.name, l.value))
            .collect::<Vec<_>>()
            .join(",");
        let flag = if ts_gap != 0 || val_gap != 0 {
            format!(" ALIGNMENT_GAP(ts_page_gap={ts_gap}, val_page_gap={val_gap})")
        } else {
            String::new()
        };
        println!(
            "  series_id={} labels={} sample_count={} min_ts_ns={} max_ts_ns={} \
             ts_range=[{}, {}) val_range=[{}, {}){}",
            hex::encode(entry.series_id.0),
            labels_str,
            entry.sample_count,
            entry.min_ts_ns,
            entry.max_ts_ns,
            range.ts_range.0,
            range.ts_range.0.saturating_add(range.ts_range.1),
            range.val_range.0,
            range.val_range.0.saturating_add(range.val_range.1),
            flag
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
