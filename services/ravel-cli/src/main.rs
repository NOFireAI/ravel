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

fn segment_inspect(bytes: &[u8]) -> anyhow::Result<()> {
    let limits = ravel_segment::ReaderLimits::default();
    let location = ravel_segment::open_from_full(bytes, limits)
        .map_err(|err| anyhow::anyhow!("failed to parse segment: {err}"))?;
    let footer = &location.footer;

    println!("total_size: {}", location.total_size);
    println!("trailer_offset: {}", location.trailer_offset);
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
        println!(
            "  kind={} offset={} len={} uncompressed_len={} comp={:?}",
            section.kind, section.offset, section.len, section.uncompressed_len, section.comp
        );
    }

    let label_dict_bytes = section_bytes(bytes, footer, SECTION_KIND_LABEL_DICT)?;
    let series_table_bytes = section_bytes(bytes, footer, SECTION_KIND_SERIES_TABLE)?;
    let entries =
        ravel_segment::decode_catalog(footer, label_dict_bytes, series_table_bytes, limits)
            .map_err(|err| anyhow::anyhow!("failed to decode series catalog: {err}"))?;
    println!("series_count (decoded): {}", entries.len());

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
