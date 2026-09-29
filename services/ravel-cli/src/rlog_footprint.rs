//! `ravel-cli rlog footprint`: attributes every stored byte of a set of RLOG
//! objects to its section, column and encoding (docs/log-segment-format.md).
//!
//! Each object is read by range: its trailer, its footer, and the FIELD_DIR and
//! PAGE_DIR sections the footer locates. Page bodies are never fetched; a
//! page's stored and uncompressed sizes and its encoding tag come from its
//! PAGE_DIR descriptor.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use ravel_logseg::encoding::Enc;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, LogFooter, SectionDesc, TRAILER_LEN, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::reader::read_section_from;
use ravel_logseg::record::{
    COL_ATTRS_RAW, COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT,
    COL_SPAN_ID, COL_STREAM_REF, COL_TRACE_ID, COL_TS, FieldType,
};
use ravel_logseg::{RlogConfig, SparseObject};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantId, TimeRange};
use serde::Serialize;

use crate::store::{StoreSelection, require_tenant_data_present};

// FIELD_DIR entry cap, matching `RlogReader`'s own decode limit.
const MAX_FIELDS: u64 = 1 << 20;

/// Stored bytes of one section kind, or of the footer or trailer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SectionBytes {
    /// Objects carrying this section.
    pub count: u64,
    /// Stored bytes (`Section.len`; the footer and trailer lengths for those
    /// two entries).
    pub bytes: u64,
    /// Bytes before section-level compression (`Section.uncompressed_len`).
    /// BLOCKS, BLOOM and POSTINGS are not compressed as a unit, so their
    /// figure equals `bytes`.
    pub uncompressed_bytes: u64,
}

/// Page bytes of one column, or of one encoding within one column.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PageBytes {
    pub pages: u64,
    /// Stored page bytes (PAGE_DIR `len`), after the page compression envelope.
    pub stored_bytes: u64,
    /// Page bytes before compression (PAGE_DIR `uncomp_len`).
    pub uncompressed_bytes: u64,
}

impl PageBytes {
    fn add(&mut self, other: &PageBytes) {
        self.pages += other.pages;
        self.stored_bytes += other.stored_bytes;
        self.uncompressed_bytes += other.uncompressed_bytes;
    }
}

/// One column's pages, keyed across objects by `(name, type)`: a dynamic
/// column's id differs between objects, its name and type do not.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ColumnFootprint {
    /// A fixed column's name, or the FIELD_DIR name of a dynamic one.
    pub name: String,
    /// `fixed` for the reserved columns 0..=9, else the FIELD_DIR type.
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(flatten)]
    pub total: PageBytes,
    /// Keyed by encoding name (docs/log-segment-format.md tag registry).
    pub encodings: BTreeMap<String, PageBytes>,
}

/// The footprint of one object, or the sum over several.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Footprint {
    pub object_count: u64,
    pub record_count: u64,
    pub total_bytes: u64,
    /// Keyed by section name, plus `FOOTER` and `TRAILER`.
    pub sections: BTreeMap<String, SectionBytes>,
    /// Bytes inside the object that no section, the footer or the trailer
    /// covers (the zero padding the format permits between sections).
    pub gap_bytes: u64,
    /// Keyed by `name:type`.
    pub columns: BTreeMap<String, ColumnFootprint>,
}

impl Footprint {
    fn add(&mut self, other: &Footprint) {
        self.object_count += other.object_count;
        self.record_count += other.record_count;
        self.total_bytes += other.total_bytes;
        self.gap_bytes += other.gap_bytes;
        for (name, s) in &other.sections {
            let into = self.sections.entry(name.clone()).or_default();
            into.count += s.count;
            into.bytes += s.bytes;
            into.uncompressed_bytes += s.uncompressed_bytes;
        }
        for (key, c) in &other.columns {
            let into = self
                .columns
                .entry(key.clone())
                .or_insert_with(|| ColumnFootprint {
                    name: c.name.clone(),
                    ty: c.ty.clone(),
                    ..ColumnFootprint::default()
                });
            into.total.add(&c.total);
            for (enc, p) in &c.encodings {
                into.encodings.entry(enc.clone()).or_default().add(p);
            }
        }
    }

    /// Stored page bytes summed over every column.
    pub fn page_stored_bytes(&self) -> u64 {
        self.columns.values().map(|c| c.total.stored_bytes).sum()
    }
}

/// One object's footprint plus what identifies it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ObjectFootprint {
    pub key: String,
    /// The version the object's own trailer carries.
    pub trailer_version: u16,
    pub level: u32,
    #[serde(flatten)]
    pub footprint: Footprint,
}

/// The whole report: every object, and their sum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FootprintReport {
    pub total: Footprint,
    pub objects: Vec<ObjectFootprint>,
}

/// An object whose figures do not add up. The report is refused rather than
/// printed, since a footprint that does not reconcile does not measure the
/// object it names.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReconcileError {
    /// Section, footer and trailer bytes plus the bytes none of them covers
    /// differ from the object size, which happens when two extents overlap.
    #[error(
        "{object}: sections, footer, trailer and uncovered bytes account for \
         {accounted_bytes} bytes but the object is {object_bytes} bytes"
    )]
    Sections {
        object: String,
        accounted_bytes: u64,
        object_bytes: u64,
    },
    /// The stored page bytes PAGE_DIR lists differ from the BLOCKS length.
    #[error("{object}: PAGE_DIR pages store {page_bytes} bytes but BLOCKS is {blocks_bytes} bytes")]
    Pages {
        object: String,
        page_bytes: u64,
        blocks_bytes: u64,
    },
}

/// Where one object's bytes come from.
pub enum ObjectSource<'a> {
    Local(&'a Path),
    Store(&'a dyn ObjectStoreBackend, &'a str),
}

impl ObjectSource<'_> {
    fn label(&self) -> String {
        match self {
            ObjectSource::Local(p) => p.display().to_string(),
            ObjectSource::Store(_, key) => (*key).to_string(),
        }
    }

    /// The last `n` bytes and the object's total size.
    async fn suffix(&self, n: u64) -> anyhow::Result<(Bytes, u64)> {
        match self {
            ObjectSource::Local(path) => {
                let mut file = std::fs::File::open(path)
                    .map_err(|err| anyhow::anyhow!("failed to open {}: {err}", path.display()))?;
                let total = file
                    .metadata()
                    .map_err(|err| anyhow::anyhow!("failed to stat {}: {err}", path.display()))?
                    .len();
                let start = total.saturating_sub(n);
                Ok((read_local(&mut file, path, start, total)?, total))
            }
            ObjectSource::Store(store, key) => {
                let got = store
                    .get(key, GetRange::Suffix(n))
                    .await
                    .map_err(|err| anyhow::anyhow!("failed to fetch {key}: {err}"))?;
                Ok((got.data, got.total_size))
            }
        }
    }

    /// Bytes `[start, end)`.
    async fn range(&self, start: u64, end: u64) -> anyhow::Result<Bytes> {
        match self {
            ObjectSource::Local(path) => {
                let mut file = std::fs::File::open(path)
                    .map_err(|err| anyhow::anyhow!("failed to open {}: {err}", path.display()))?;
                read_local(&mut file, path, start, end)
            }
            ObjectSource::Store(store, key) => Ok(store
                .get(key, GetRange::Range(start, end))
                .await
                .map_err(|err| anyhow::anyhow!("failed to fetch {key}: {err}"))?
                .data),
        }
    }
}

fn read_local(
    file: &mut std::fs::File,
    path: &Path,
    start: u64,
    end: u64,
) -> anyhow::Result<Bytes> {
    let len = usize::try_from(end.saturating_sub(start))
        .map_err(|_| anyhow::anyhow!("range too large in {}", path.display()))?;
    let mut buf = vec![0u8; len];
    file.seek(SeekFrom::Start(start))
        .and_then(|_| file.read_exact(&mut buf))
        .map_err(|err| anyhow::anyhow!("failed to read {}: {err}", path.display()))?;
    Ok(Bytes::from(buf))
}

/// Places `[start, end)` into `sparse`, fetching it only if no region placed
/// so far already holds it.
async fn ensure_placed(
    src: &ObjectSource<'_>,
    sparse: &mut SparseObject,
    start: u64,
    end: u64,
) -> anyhow::Result<()> {
    if start >= end || sparse.holds_in_one_region(start, end) {
        return Ok(());
    }
    let bytes = src.range(start, end).await?;
    sparse
        .place(start, bytes)
        .map_err(|err| anyhow::anyhow!("{}: {err}", src.label()))
}

fn section_range(desc: &SectionDesc) -> anyhow::Result<(u64, u64)> {
    let end = desc
        .offset
        .checked_add(desc.len)
        .ok_or_else(|| anyhow::anyhow!("section kind {} extent overflows", desc.kind))?;
    Ok((desc.offset, end))
}

/// Measures one object from its trailer, footer, FIELD_DIR and PAGE_DIR.
pub async fn object_footprint(src: &ObjectSource<'_>) -> anyhow::Result<ObjectFootprint> {
    let label = src.label();
    let ctx = |what: &str, err: &dyn std::fmt::Display| anyhow::anyhow!("{label}: {what}: {err}");

    let (trailer, total) = src.suffix(TRAILER_LEN as u64).await?;
    let trailer_version = footer::trailer_version(&trailer).map_err(|e| ctx("trailer", &e))?;
    let mut sparse = SparseObject::new(total);
    sparse
        .place(total.saturating_sub(trailer.len() as u64), trailer.clone())
        .map_err(|e| ctx("trailer", &e))?;
    let footer_len = trailer
        .get(0..4)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| anyhow::anyhow!("{label}: object smaller than trailer"))?;
    let footer_start = total
        .checked_sub(TRAILER_LEN as u64)
        .and_then(|v| v.checked_sub(u64::from(footer_len)))
        .ok_or_else(|| anyhow::anyhow!("{label}: footer_len past object start"))?;
    ensure_placed(src, &mut sparse, footer_start, total - TRAILER_LEN as u64).await?;
    let footer = footer::open_source(&sparse).map_err(|e| ctx("footer", &e))?;

    let section = |k: u32| {
        footer
            .section(k)
            .ok_or_else(|| anyhow::anyhow!("{label}: missing section kind {k}"))
    };
    let cfg = RlogConfig::default();

    let field_desc = section(kind::FIELD_DIR)?;
    let (s, e) = section_range(field_desc)?;
    ensure_placed(src, &mut sparse, s, e).await?;
    let field_raw =
        read_section_from(&sparse, field_desc, &cfg).map_err(|e| ctx("FIELD_DIR", &e))?;
    let field_dir = FieldDir::decode(&field_raw, MAX_FIELDS).map_err(|e| ctx("FIELD_DIR", &e))?;

    let page_desc = section(kind::PAGE_DIR)?;
    let (s, e) = section_range(page_desc)?;
    ensure_placed(src, &mut sparse, s, e).await?;
    let page_raw = read_section_from(&sparse, page_desc, &cfg).map_err(|e| ctx("PAGE_DIR", &e))?;
    let page_dir = PageDir::decode(&page_raw).map_err(|e| ctx("PAGE_DIR", &e))?;
    let blocks = section(kind::BLOCKS)?;
    page_dir
        .validate_extents(blocks.len)
        .map_err(|e| ctx("PAGE_DIR", &e))?;
    if page_dir.block_count() != footer.block_count {
        anyhow::bail!(
            "{label}: PAGE_DIR covers {} blocks but the footer counts {}",
            page_dir.block_count(),
            footer.block_count
        );
    }

    let footprint = tally(
        &footer,
        footer_start,
        footer_len,
        total,
        &field_dir,
        &page_dir,
    );
    let accounted_bytes = footprint
        .sections
        .values()
        .try_fold(footprint.gap_bytes, |acc, s| acc.checked_add(s.bytes))
        .unwrap_or(u64::MAX);
    if accounted_bytes != total {
        return Err(ReconcileError::Sections {
            object: label,
            accounted_bytes,
            object_bytes: total,
        }
        .into());
    }
    let page_bytes = footprint.page_stored_bytes();
    if page_bytes != blocks.len {
        return Err(ReconcileError::Pages {
            object: label,
            page_bytes,
            blocks_bytes: blocks.len,
        }
        .into());
    }

    Ok(ObjectFootprint {
        key: label,
        trailer_version,
        level: footer.level,
        footprint,
    })
}

/// The bytes of `[0, total)` that no extent covers. Overlapping extents are
/// counted once here, so their summed lengths exceed `total - uncovered`.
fn uncovered_bytes(mut extents: Vec<(u64, u64)>, total: u64) -> u64 {
    extents.sort_unstable();
    let mut cursor = 0u64;
    let mut uncovered = 0u64;
    for (start, end) in extents {
        if start > cursor {
            uncovered += start - cursor;
        }
        cursor = cursor.max(end);
    }
    uncovered + total.saturating_sub(cursor)
}

fn tally(
    footer: &LogFooter,
    footer_start: u64,
    footer_len: u32,
    total: u64,
    field_dir: &FieldDir,
    page_dir: &PageDir,
) -> Footprint {
    let mut fp = Footprint {
        object_count: 1,
        record_count: footer.record_count,
        total_bytes: total,
        ..Footprint::default()
    };
    let mut extents = vec![
        (footer_start, total - TRAILER_LEN as u64),
        (total - TRAILER_LEN as u64, total),
    ];
    for s in &footer.sections {
        let entry = fp.sections.entry(section_name(s.kind)).or_default();
        entry.count += 1;
        entry.bytes += s.len;
        entry.uncompressed_bytes += if s.comp == footer::COMP_NONE {
            s.len
        } else {
            s.uncomp_len
        };
        extents.push((s.offset, s.offset.saturating_add(s.len)));
    }
    for (name, len) in [
        ("FOOTER", u64::from(footer_len)),
        ("TRAILER", TRAILER_LEN as u64),
    ] {
        fp.sections.insert(
            name.to_string(),
            SectionBytes {
                count: 1,
                bytes: len,
                uncompressed_bytes: len,
            },
        );
    }
    fp.gap_bytes = uncovered_bytes(extents, total);

    for group in &page_dir.groups {
        for chunk in &group.chunks {
            let (name, ty) = column_name(chunk.column_id, field_dir);
            let col = fp
                .columns
                .entry(format!("{name}:{ty}"))
                .or_insert_with(|| ColumnFootprint {
                    name,
                    ty,
                    ..ColumnFootprint::default()
                });
            for page in &chunk.pages {
                let one = PageBytes {
                    pages: 1,
                    stored_bytes: page.len,
                    uncompressed_bytes: page.uncomp_len,
                };
                col.total.add(&one);
                col.encodings
                    .entry(enc_name(page.enc).to_string())
                    .or_default()
                    .add(&one);
            }
        }
    }
    fp
}

fn section_name(k: u32) -> String {
    match k {
        kind::STREAM_DIR => "STREAM_DIR".to_string(),
        kind::FIELD_DIR => "FIELD_DIR".to_string(),
        kind::BLOCKS => "BLOCKS".to_string(),
        kind::SKIP_IDX => "SKIP_IDX".to_string(),
        kind::BLOOM => "BLOOM".to_string(),
        kind::POSTINGS => "POSTINGS".to_string(),
        kind::PAGE_DIR => "PAGE_DIR".to_string(),
        other => format!("UNKNOWN_{other}"),
    }
}

fn column_name(column_id: u32, field_dir: &FieldDir) -> (String, String) {
    let fixed = match column_id {
        COL_TS => Some("ts"),
        COL_OBSERVED_TS => Some("observed_ts"),
        COL_STREAM_REF => Some("stream_ref"),
        COL_SEVERITY_NUM => Some("severity_num"),
        COL_SEVERITY_TEXT => Some("severity_text"),
        COL_BODY => Some("body"),
        COL_TRACE_ID => Some("trace_id"),
        COL_SPAN_ID => Some("span_id"),
        COL_FLAGS => Some("flags"),
        COL_ATTRS_RAW => Some("attrs_raw"),
        _ => None,
    };
    if let Some(name) = fixed {
        return (name.to_string(), "fixed".to_string());
    }
    match field_dir.by_column_id(column_id) {
        Some(entry) => (entry.name.clone(), field_type_name(entry.ty).to_string()),
        // PAGE_DIR naming a column FIELD_DIR does not: still counted, by id.
        None => (format!("column_{column_id}"), "unknown".to_string()),
    }
}

fn field_type_name(ty: FieldType) -> &'static str {
    match ty {
        FieldType::Str => "str",
        FieldType::I64 => "i64",
        FieldType::F64 => "f64",
        FieldType::Bool => "bool",
        FieldType::Bytes => "bytes",
    }
}

/// The docs/log-segment-format.md tag registry name of an encoding.
pub fn enc_name(enc: Enc) -> &'static str {
    match enc {
        Enc::Plain => "plain",
        Enc::Constant => "constant",
        Enc::Rle => "rle",
        Enc::DeltaZigzag => "delta_zigzag",
        Enc::DoubleDelta => "double_delta",
        Enc::ForBitpack => "for_bitpack",
        Enc::Dict => "dictionary",
        Enc::Bitmap => "bitmap",
        Enc::FixedWidth => "fixed_width",
    }
}

/// Sums per-object footprints into a report.
pub fn report(objects: Vec<ObjectFootprint>) -> FootprintReport {
    let mut total = Footprint::default();
    for o in &objects {
        total.add(&o.footprint);
    }
    FootprintReport { total, objects }
}

/// The data object keys of every logs segment the catalog resolves for
/// `tenant` over all time: live L0 flush and L1 compacted segments.
pub async fn tenant_object_keys(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    shards: u32,
    now_ns: i64,
) -> anyhow::Result<Vec<String>> {
    let tenant_hash = TenantId::new(tenant).hash();
    require_tenant_data_present(
        selection,
        store.as_ref(),
        "rlog footprint",
        tenant,
        &tenant_hash,
    )
    .await?;
    let config = ravel_catalog::CatalogConfig {
        shard_count: shards,
        ..ravel_catalog::CatalogConfig::default()
    };
    let catalog = ravel_catalog::Catalog::new(store, config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();
    let range = TimeRange {
        start_ns: i64::MIN,
        end_ns: i64::MAX,
    };
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, range, &[], now_ns)
        .await
        .map_err(|err| anyhow::anyhow!("failed to resolve catalog: {err}"))?;
    let mut keys: Vec<String> = snapshot
        .segments
        .into_iter()
        .map(|s| s.data_object_key)
        .collect();
    keys.sort();
    Ok(keys)
}

/// Measures every object in `targets`, as given on the command line: a target
/// naming an existing local file is read from disk, any other is an object key
/// in `store`.
pub async fn footprint_targets(
    store: &dyn ObjectStoreBackend,
    targets: &[String],
) -> anyhow::Result<FootprintReport> {
    let mut objects = Vec::with_capacity(targets.len());
    for t in targets {
        let path = Path::new(t);
        let src = if path.is_file() {
            ObjectSource::Local(path)
        } else {
            ObjectSource::Store(store, t)
        };
        objects.push(object_footprint(&src).await?);
    }
    Ok(report(objects))
}

/// Measures every object key in `keys` from `store`, whatever the local
/// filesystem holds at the same path. This is the form for keys the catalog
/// resolved.
pub async fn footprint_keys(
    store: &dyn ObjectStoreBackend,
    keys: &[String],
) -> anyhow::Result<FootprintReport> {
    let mut objects = Vec::with_capacity(keys.len());
    for k in keys {
        objects.push(object_footprint(&ObjectSource::Store(store, k)).await?);
    }
    Ok(report(objects))
}

/// The text form of a report.
pub fn render_text(r: &FootprintReport) -> String {
    use std::fmt::Write;
    let t = &r.total;
    let mut out = String::new();
    let _ = writeln!(out, "object_count: {}", t.object_count);
    let _ = writeln!(out, "record_count: {}", t.record_count);
    let _ = writeln!(out, "total_bytes: {}", t.total_bytes);
    let _ = writeln!(out, "gap_bytes: {}", t.gap_bytes);
    let _ = writeln!(out, "page_stored_bytes: {}", t.page_stored_bytes());
    let _ = writeln!(out, "objects:");
    for o in &r.objects {
        let _ = writeln!(
            out,
            "  {} version={} level={} records={} bytes={}",
            o.key, o.trailer_version, o.level, o.footprint.record_count, o.footprint.total_bytes
        );
    }
    let _ = writeln!(out, "sections:");
    for (name, s) in &t.sections {
        let _ = writeln!(
            out,
            "  {name} count={} bytes={} uncompressed_bytes={}",
            s.count, s.bytes, s.uncompressed_bytes
        );
    }
    let _ = writeln!(out, "columns:");
    for c in t.columns.values() {
        let _ = writeln!(
            out,
            "  {} type={} pages={} stored_bytes={} uncompressed_bytes={}",
            c.name, c.ty, c.total.pages, c.total.stored_bytes, c.total.uncompressed_bytes
        );
        for (enc, p) in &c.encodings {
            let _ = writeln!(
                out,
                "    enc={enc} pages={} stored_bytes={} uncompressed_bytes={}",
                p.pages, p.stored_bytes, p.uncompressed_bytes
            );
        }
    }
    out
}
