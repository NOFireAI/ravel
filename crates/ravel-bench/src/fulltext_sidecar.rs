//! T13 arm 2 (issue #3711, ADR-3692 work package 4): an optional compressed
//! full-text sidecar for cold logs in the sense of LogCloud (PVLDB vol 18
//! p2362), against the production per-block bloom and a block-level body
//! postings section, for rare-token search.
//!
//! The sidecar is one object per RLOG object: a POSTINGS section built by the
//! production encoder ([`encode_postings_section`]) whose body field maps each
//! token found in at most `row_level_max_rows` rows to the object-wide row
//! ordinals holding it, and whose [`BLOCK_LEVEL_FIELD`] maps every other
//! token to the blocks holding it, followed by the object's [`RowMap`], which
//! turns an ordinal back into `(block, row within block)`. A rare-token query
//! reads the sidecar's lookup (header plus one term block) per object, and
//! the directories and body pages of the matched objects only.
//!
//! Per object, measured on the production writer's output:
//!
//! - the BLOOM section's real length and its verdicts: the blocks a
//!   `HasWord` scan of the public reader leaves after pruning;
//! - the block-level body postings the sidecar replaces, from the production
//!   encoder over the same tokens the bloom indexes, probed through
//!   [`PostingsSection::probe`];
//! - the sidecar's size, its build CPU (body decode, tokenize, encode)
//!   against the CPU of a compaction rewrite of the same object (full
//!   decode, production writer), and its probe verdicts mapped through the
//!   row map, checked row for row against the truth;
//! - the deletion lifecycle: one seeded row deleted, the object rewritten by
//!   the production writer and the sidecar rebuilt from it, with the rebuild
//!   CPU and a residue check that every row the rebuilt sidecar names for
//!   the deleted row's tokens holds the token.
//!
//! The 100 GB figures are [`extrapolate`]d from those per-object means;
//! [`FORMULA`] states the formula and every input is a reported figure.

use std::collections::{BTreeMap, BTreeSet};

use ravel_logseg::postings::{FieldTerms, PostingsSection, encode_postings_section};
use ravel_logseg::record::COL_BODY;
use ravel_logseg::varint::get_uvarint;
use serde::{Deserialize, Serialize};

use crate::t13::{Decision, Expect, Figure, RowMap, SavedDecision, validate_figure_names};

pub const ARM: &str = "fulltext_sidecar";

/// GO thresholds (issue #3711).
pub const MAX_RARE_BYTES_RATIO: f64 = 0.1;
pub const MAX_INDEX_RATIO: f64 = 1.2;
pub const MAX_BUILD_CPU_RATIO: f64 = 1.5;

/// The sidecar field holding tokens too common for row-level postings.
pub const BLOCK_LEVEL_FIELD: u32 = 10_000;

/// Raw corpus bytes the rare-token figures are extrapolated to.
pub const EXTRAPOLATE_RAW_BYTES: f64 = 100e9;

pub const DECISION_RULE: &str = "A subject is GO when rare_bytes_ratio <= 0.1 (the sidecar \
path's extrapolated bytes per rare-token query over the bloom path's), index_ratio <= 1.2 \
(sidecar bytes over the block-level body postings it replaces), build_cpu_ratio <= 1.5 \
(sidecar build CPU over compaction CPU), the deletion path is costed \
(deletion/rebuild_cpu_ratio reported) and deletion/residue_rows is 0. The arm is GO when \
every pre-registered subject is GO.";

pub const FORMULA: &str = "Per rare-token query at X = 100e9 raw corpus bytes: \
N = X / (raw_bytes / objects) objects of B = blocks / objects blocks each; \
D = dirs bytes per object, L = bloom bytes per object, R = rowmap bytes per object, \
P = body_page_mean, T = truth_blocks_mean, f = bloom fp_rate, \
Lp = lookup/postings_mean, Ls = lookup/sidecar_mean. \
bloom = N*(D + L) + (T + f*(N*B - T))*P; \
postings = N*(D + Lp) + T*P; \
sidecar = N*Ls + T*(D + R + P) (at most T objects hold a match).";

/// Every figure a subject reports, keyed without the subject prefix.
pub fn figure_names() -> Vec<String> {
    [
        "object/count",
        "object/lines",
        "object/raw_bytes",
        "object/bytes",
        "object/blocks",
        "truth_blocks_mean",
        "bloom/survivors_mean",
        "bloom/fp_rate",
        "bytes/dirs",
        "bytes/bloom",
        "bytes/postings",
        "bytes/sidecar",
        "bytes/rowmap",
        "bytes/body_page_mean",
        "lookup/postings_mean",
        "lookup/sidecar_mean",
        "cpu/compaction_ms",
        "cpu/build_ms",
        "build_cpu_ratio",
        "index_ratio",
        "rewrite_amp",
        "extrap/objects",
        "extrap/bloom_bytes",
        "extrap/postings_bytes",
        "extrap/sidecar_bytes",
        "rare_bytes_ratio",
        "deletion/rebuild_cpu_ms",
        "deletion/rebuild_cpu_ratio",
        "deletion/residue_rows",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub schema: u32,
    pub arm: String,
    pub subject: Vec<SubjectSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectSpec {
    pub name: String,
    pub corpus: String,
    pub source: String,
    /// The corpus file's sha256; any other file is refused before it is read.
    pub corpus_sha256: String,
    pub max_lines: u64,
    /// Objects written, each from the next `lines_per_object` corpus lines.
    pub objects: usize,
    pub lines_per_object: usize,
    pub block_records: usize,
    pub zstd_level: i32,
    /// Terms per POSTINGS term block, in both the baseline and the sidecar.
    pub stride: u32,
    /// Tokens in at most this many rows of an object get row-level postings.
    pub row_level_max_rows: usize,
    /// A query token is one found in `rare_min_rows..=rare_max_rows` rows of
    /// its object.
    pub rare_min_rows: usize,
    pub rare_max_rows: usize,
    pub queries_per_object: usize,
    pub seed: u64,
    /// CPU figures are the median of this many runs.
    pub reps: usize,
    pub figures: BTreeMap<String, Expect>,
}

pub fn parse(text: &str) -> Result<Spec, String> {
    let spec: Spec = toml::from_str(text).map_err(|e| format!("{ARM} spec: {e}"))?;
    if spec.schema != 1 || spec.arm != ARM {
        return Err(format!(
            "{ARM} spec: schema {} arm {:?} is not schema 1 arm {ARM:?}",
            spec.schema, spec.arm
        ));
    }
    if spec.subject.is_empty() {
        return Err(format!("{ARM} spec: no subject"));
    }
    let mut names = BTreeSet::new();
    for s in &spec.subject {
        if !names.insert(s.name.as_str()) {
            return Err(format!("{ARM} spec: subject {} twice", s.name));
        }
        s.validate()?;
    }
    Ok(spec)
}

impl SubjectSpec {
    fn validate(&self) -> Result<(), String> {
        let at = |m: &str| format!("{ARM} spec: subject {}: {m}", self.name);
        if self.objects == 0
            || self.lines_per_object == 0
            || self.block_records == 0
            || self.stride == 0
            || self.queries_per_object == 0
            || self.reps == 0
            || self.rare_min_rows == 0
        {
            return Err(at(
                "objects, lines_per_object, block_records, stride, queries_per_object, reps \
                 and rare_min_rows must be nonzero",
            ));
        }
        if self.rare_min_rows > self.rare_max_rows || self.rare_max_rows > self.row_level_max_rows {
            return Err(at(
                "rare_min_rows <= rare_max_rows <= row_level_max_rows must hold",
            ));
        }
        if (self.objects as u64).saturating_mul(self.lines_per_object as u64) > self.max_lines {
            return Err(at("objects * lines_per_object exceeds max_lines"));
        }
        if self.corpus_sha256.len() != 64 {
            return Err(at("corpus_sha256 must be 64 hex digits"));
        }
        validate_figure_names(
            &format!("{ARM} spec: subject {}", self.name),
            &self.figures,
            &figure_names(),
        )
    }

    /// Keyed `<subject>/<figure>`.
    pub fn expectations(&self) -> BTreeMap<String, Expect> {
        self.figures
            .iter()
            .map(|(k, v)| (format!("{}/{k}", self.name), v.clone()))
            .collect()
    }
}

/// One object's tokens: per block, per row, the row's body tokens.
pub type ObjectTokens = Vec<Vec<Vec<Vec<u8>>>>;

/// Token to the set of blocks holding it.
pub fn block_terms(tokens: &ObjectTokens) -> BTreeMap<Vec<u8>, BTreeSet<u32>> {
    let mut out: BTreeMap<Vec<u8>, BTreeSet<u32>> = BTreeMap::new();
    for (b, rows) in tokens.iter().enumerate() {
        for row in rows {
            for t in row {
                out.entry(t.clone()).or_default().insert(b as u32);
            }
        }
    }
    out
}

/// Token to the object-wide row ordinals holding it, ordinals counted
/// across blocks in block order.
pub fn row_terms(tokens: &ObjectTokens) -> BTreeMap<Vec<u8>, BTreeSet<u32>> {
    let mut out: BTreeMap<Vec<u8>, BTreeSet<u32>> = BTreeMap::new();
    let mut ordinal = 0u32;
    for rows in tokens {
        for row in rows {
            for t in row {
                out.entry(t.clone()).or_default().insert(ordinal);
            }
            ordinal += 1;
        }
    }
    out
}

/// The block-level body postings the sidecar is compared against.
pub fn build_postings(
    tokens: &ObjectTokens,
    stride: u32,
    zstd_level: i32,
) -> Result<Vec<u8>, String> {
    let mut fields = BTreeMap::new();
    fields.insert(COL_BODY, FieldTerms::Terms(block_terms(tokens)));
    encode_postings_section(&fields, stride, zstd_level).map_err(|e| format!("postings: {e}"))
}

/// The sidecar: its POSTINGS section and its encoded row map.
pub struct Sidecar {
    pub section: Vec<u8>,
    pub rowmap: Vec<u8>,
}

impl Sidecar {
    pub fn len(&self) -> u64 {
        (self.section.len() + self.rowmap.len()) as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn build_sidecar(
    tokens: &ObjectTokens,
    row_level_max_rows: usize,
    stride: u32,
    zstd_level: i32,
) -> Result<Sidecar, String> {
    let blocks = block_terms(tokens);
    let mut rows = BTreeMap::new();
    let mut common = BTreeMap::new();
    for (t, ordinals) in row_terms(tokens) {
        if ordinals.len() <= row_level_max_rows {
            rows.insert(t, ordinals);
        } else if let Some(b) = blocks.get(&t) {
            common.insert(t, b.clone());
        }
    }
    let mut fields = BTreeMap::new();
    fields.insert(COL_BODY, FieldTerms::Terms(rows));
    fields.insert(BLOCK_LEVEL_FIELD, FieldTerms::Terms(common));
    let section = encode_postings_section(&fields, stride, zstd_level)
        .map_err(|e| format!("sidecar: {e}"))?;
    let counts: Vec<u32> = tokens.iter().map(|b| b.len() as u32).collect();
    Ok(Sidecar {
        section,
        rowmap: RowMap::new(&counts).encode(),
    })
}

/// What a sidecar probe names for a token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidecarHit {
    /// `(block, row within block)` of every row holding it.
    Rows(BTreeSet<(u32, u32)>),
    /// The blocks holding a token too common for row-level postings.
    Blocks(BTreeSet<u32>),
    Absent,
}

/// Probes the sidecar for `token` and maps row-level ordinals through its
/// row map.
pub fn probe_sidecar(sidecar: &Sidecar, token: &[u8]) -> Result<SidecarHit, String> {
    let section = PostingsSection::parse(&sidecar.section).map_err(|e| format!("sidecar: {e}"))?;
    let map = RowMap::decode(&sidecar.rowmap)?;
    let probe = |col: u32| {
        section
            .probe(col, token)
            .map_err(|e| format!("sidecar probe: {e}"))
    };
    if let Some(ordinals) = probe(COL_BODY)?.filter(|v| !v.is_empty()) {
        let mut rows = BTreeSet::new();
        for o in ordinals {
            rows.insert(
                map.locate(u64::from(o))
                    .ok_or_else(|| format!("sidecar ordinal {o} past the row map"))?,
            );
        }
        return Ok(SidecarHit::Rows(rows));
    }
    if let Some(blocks) = probe(BLOCK_LEVEL_FIELD)?.filter(|v| !v.is_empty()) {
        return Ok(SidecarHit::Blocks(blocks.into_iter().collect()));
    }
    Ok(SidecarHit::Absent)
}

/// After a deletion, the rows `sidecar` names for each of the deleted row's
/// `deleted` tokens that do not hold the token in the rewritten object's
/// `after` tokens (a block-level hit counts when no row of the block holds
/// it). Zero for a sidecar rebuilt from the rewritten object.
pub fn residue(
    sidecar: &Sidecar,
    after: &ObjectTokens,
    deleted: &[Vec<u8>],
) -> Result<u64, String> {
    let holds = |b: u32, r: u32, t: &[u8]| {
        after
            .get(b as usize)
            .and_then(|rows| rows.get(r as usize))
            .is_some_and(|row| row.iter().any(|x| x == t))
    };
    let mut bad = 0u64;
    let distinct: BTreeSet<&Vec<u8>> = deleted.iter().collect();
    for t in distinct {
        match probe_sidecar(sidecar, t)? {
            SidecarHit::Rows(rows) => {
                bad += rows.iter().filter(|(b, r)| !holds(*b, *r, t)).count() as u64;
            }
            SidecarHit::Blocks(blocks) => {
                bad += blocks
                    .iter()
                    .filter(|&&b| {
                        let n = after.get(b as usize).map_or(0, Vec::len) as u32;
                        !(0..n).any(|r| holds(b, r, t))
                    })
                    .count() as u64;
            }
            SidecarHit::Absent => {}
        }
    }
    Ok(bad)
}

/// One field's sparse index entry: a term block's first term and stored
/// length.
struct BlockEntry {
    first_term: Vec<u8>,
    stored_len: u64,
}

/// Bytes a ranged reader fetches to probe `section` for `token` in each of
/// `columns` in turn, stopping at the first field that holds it: the whole
/// header (version, field table and every sparse index), then per field
/// probed the stored bytes of the one term block whose range covers the
/// token (none when the token sorts before the first block). The header
/// layout is the POSTINGS v2 grammar of docs/adrs/0049-rlog-postings.md.
pub fn lookup_bytes(section: &[u8], columns: &[u32], token: &[u8]) -> Result<u64, String> {
    let bad = |m: &str| format!("postings header: {m}");
    let mut pos = 1usize;
    let count = section
        .get(1..5)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| bad("short"))?;
    pos += 4;
    let uv = |pos: &mut usize| get_uvarint(section, pos).map_err(|e| bad(&e.to_string()));
    let mut fields: BTreeMap<u32, Vec<BlockEntry>> = BTreeMap::new();
    for _ in 0..count {
        let col = u32::try_from(uv(&mut pos)?).map_err(|_| bad("column id"))?;
        let capped = *section.get(pos).ok_or_else(|| bad("short"))? != 0;
        pos += 1;
        let mut entries = Vec::new();
        if !capped {
            let _stride = uv(&mut pos)?;
            let _terms = uv(&mut pos)?;
            let n = uv(&mut pos)?;
            for _ in 0..n {
                let len = usize::try_from(uv(&mut pos)?).map_err(|_| bad("term length"))?;
                let first_term = section
                    .get(pos..pos + len)
                    .ok_or_else(|| bad("short"))?
                    .to_vec();
                pos += len + 8;
                let stored_len = section
                    .get(pos..pos + 8)
                    .and_then(|b| <[u8; 8]>::try_from(b).ok())
                    .map(u64::from_le_bytes)
                    .ok_or_else(|| bad("short"))?;
                pos += 8 + 8 + 4;
                entries.push(BlockEntry {
                    first_term,
                    stored_len,
                });
            }
        }
        fields.insert(col, entries);
    }
    let mut total = pos as u64;
    let parsed = PostingsSection::parse(section).map_err(|e| format!("postings: {e}"))?;
    for col in columns {
        let Some(entries) = fields.get(col) else {
            continue;
        };
        if let Some(e) = entries
            .iter()
            .take_while(|e| e.first_term.as_slice() <= token)
            .last()
        {
            total += e.stored_len;
        }
        let hit = parsed
            .probe(*col, token)
            .map_err(|e| format!("postings probe: {e}"))?;
        if hit.is_some_and(|v| !v.is_empty()) {
            break;
        }
    }
    Ok(total)
}

/// One rare-token query's measured figures on its object.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryMeasure {
    pub truth_blocks: u64,
    pub survivors: u64,
    pub postings_lookup: u64,
    pub sidecar_lookup: u64,
}

/// One object's measured figures.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObjectMeasure {
    pub lines: u64,
    pub raw_bytes: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub dirs: u64,
    pub bloom: u64,
    pub postings: u64,
    pub sidecar: u64,
    pub rowmap: u64,
    pub body_page_bytes: u64,
    pub compaction_cpu_s: f64,
    pub build_cpu_s: f64,
    pub rebuild_cpu_s: f64,
    pub residue_rows: u64,
    pub queries: Vec<QueryMeasure>,
}

/// The extrapolation inputs, each a reported figure.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Inputs {
    pub objects: f64,
    pub raw_bytes: f64,
    pub blocks: f64,
    pub dirs: f64,
    pub bloom: f64,
    pub rowmap: f64,
    pub body_page_mean: f64,
    pub truth_blocks_mean: f64,
    pub fp_rate: f64,
    pub postings_lookup_mean: f64,
    pub sidecar_lookup_mean: f64,
}

/// `(objects, bloom, postings, sidecar)` bytes per query at
/// [`EXTRAPOLATE_RAW_BYTES`], by [`FORMULA`].
pub fn extrapolate(i: &Inputs) -> (f64, f64, f64, f64) {
    let n = EXTRAPOLATE_RAW_BYTES / (i.raw_bytes / i.objects);
    let b = i.blocks / i.objects;
    let d = i.dirs / i.objects;
    let l = i.bloom / i.objects;
    let r = i.rowmap / i.objects;
    let (p, t, f) = (i.body_page_mean, i.truth_blocks_mean, i.fp_rate);
    let bloom = n * (d + l) + (t + f * (n * b - t)) * p;
    let postings = n * (d + i.postings_lookup_mean) + t * p;
    let sidecar = n * i.sidecar_lookup_mean + t * (d + r + p);
    (n, bloom, postings, sidecar)
}

fn mean(v: impl Iterator<Item = f64>) -> f64 {
    let (s, n) = v.fold((0.0, 0u64), |(s, n), x| (s + x, n + 1));
    if n == 0 { 0.0 } else { s / n as f64 }
}

/// Every figure of [`figure_names`] for `subject` from its objects.
/// `fp_rate` is false-positive survivors over the blocks a query's token is
/// not in, pooled over every query.
pub fn figures(subject: &str, objs: &[ObjectMeasure]) -> Vec<Figure> {
    let sum = |f: fn(&ObjectMeasure) -> u64| objs.iter().map(f).sum::<u64>() as f64;
    let queries: Vec<(&ObjectMeasure, &QueryMeasure)> = objs
        .iter()
        .flat_map(|o| o.queries.iter().map(move |q| (o, q)))
        .collect();
    let fp: u64 = queries
        .iter()
        .map(|(_, q)| q.survivors - q.truth_blocks)
        .sum();
    let negatives: u64 = queries.iter().map(|(o, q)| o.blocks - q.truth_blocks).sum();
    let blocks = sum(|o| o.blocks);
    let compaction_s: f64 = objs.iter().map(|o| o.compaction_cpu_s).sum();
    let build_s: f64 = objs.iter().map(|o| o.build_cpu_s).sum();
    let rebuild_s: f64 = objs.iter().map(|o| o.rebuild_cpu_s).sum();
    let inputs = Inputs {
        objects: objs.len() as f64,
        raw_bytes: sum(|o| o.raw_bytes),
        blocks,
        dirs: sum(|o| o.dirs),
        bloom: sum(|o| o.bloom),
        rowmap: sum(|o| o.rowmap),
        body_page_mean: sum(|o| o.body_page_bytes) / blocks,
        truth_blocks_mean: mean(queries.iter().map(|(_, q)| q.truth_blocks as f64)),
        fp_rate: if negatives == 0 {
            0.0
        } else {
            fp as f64 / negatives as f64
        },
        postings_lookup_mean: mean(queries.iter().map(|(_, q)| q.postings_lookup as f64)),
        sidecar_lookup_mean: mean(queries.iter().map(|(_, q)| q.sidecar_lookup as f64)),
    };
    let (n, bloom, postings, sidecar) = extrapolate(&inputs);
    let values = [
        ("object/count", inputs.objects),
        ("object/lines", sum(|o| o.lines)),
        ("object/raw_bytes", inputs.raw_bytes),
        ("object/bytes", sum(|o| o.bytes)),
        ("object/blocks", blocks),
        ("truth_blocks_mean", inputs.truth_blocks_mean),
        (
            "bloom/survivors_mean",
            mean(queries.iter().map(|(_, q)| q.survivors as f64)),
        ),
        ("bloom/fp_rate", inputs.fp_rate),
        ("bytes/dirs", inputs.dirs),
        ("bytes/bloom", inputs.bloom),
        ("bytes/postings", sum(|o| o.postings)),
        ("bytes/sidecar", sum(|o| o.sidecar)),
        ("bytes/rowmap", inputs.rowmap),
        ("bytes/body_page_mean", inputs.body_page_mean),
        ("lookup/postings_mean", inputs.postings_lookup_mean),
        ("lookup/sidecar_mean", inputs.sidecar_lookup_mean),
        ("cpu/compaction_ms", compaction_s * 1e3),
        ("cpu/build_ms", build_s * 1e3),
        ("build_cpu_ratio", build_s / compaction_s),
        ("index_ratio", sum(|o| o.sidecar) / sum(|o| o.postings)),
        (
            "rewrite_amp",
            (sum(|o| o.bytes) + sum(|o| o.sidecar)) / sum(|o| o.bytes),
        ),
        ("extrap/objects", n),
        ("extrap/bloom_bytes", bloom),
        ("extrap/postings_bytes", postings),
        ("extrap/sidecar_bytes", sidecar),
        ("rare_bytes_ratio", sidecar / bloom),
        ("deletion/rebuild_cpu_ms", rebuild_s * 1e3),
        ("deletion/rebuild_cpu_ratio", rebuild_s / compaction_s),
        ("deletion/residue_rows", sum(|o| o.residue_rows)),
    ];
    values
        .iter()
        .map(|(k, v)| Figure {
            key: format!("{subject}/{k}"),
            value: *v,
        })
        .collect()
}

fn value(figures: &[Figure], key: &str) -> Option<f64> {
    let mut it = figures.iter().filter(|f| f.key == key);
    match (it.next(), it.next()) {
        (Some(f), None) => Some(f.value),
        _ => None,
    }
}

/// The per-subject rule of [`DECISION_RULE`].
pub fn decide(subject: &str, figures: &[Figure]) -> Decision {
    let get = |f: &str| value(figures, &format!("{subject}/{f}"));
    let criteria: [(&str, Option<f64>, fn(f64) -> bool, &str); 5] = [
        (
            "rare_bytes_ratio",
            get("rare_bytes_ratio"),
            |x| x <= MAX_RARE_BYTES_RATIO,
            "<= 0.1",
        ),
        (
            "index_ratio",
            get("index_ratio"),
            |x| x <= MAX_INDEX_RATIO,
            "<= 1.2",
        ),
        (
            "build_cpu_ratio",
            get("build_cpu_ratio"),
            |x| x <= MAX_BUILD_CPU_RATIO,
            "<= 1.5",
        ),
        (
            "deletion/rebuild_cpu_ratio",
            get("deletion/rebuild_cpu_ratio"),
            f64::is_finite,
            "costed",
        ),
        (
            "deletion/residue_rows",
            get("deletion/residue_rows"),
            |x| x == 0.0,
            "== 0",
        ),
    ];
    let mut go = true;
    let mut parts = Vec::new();
    for (name, v, pass, rule) in criteria {
        let ok = v.is_some_and(pass);
        go &= ok;
        let shown = v.map_or_else(|| "missing".to_string(), |x| format!("{x:.4}"));
        parts.push(format!(
            "{name} {shown} {rule} {}",
            if ok { "ok" } else { "FAIL" }
        ));
    }
    Decision {
        subject: subject.to_string(),
        go,
        variants: Vec::new(),
        line: format!(
            "{ARM} decision {subject}: {} [{}]",
            if go { "GO" } else { "NO-GO" },
            parts.join("; ")
        ),
    }
}

/// The overall rule of [`DECISION_RULE`] over the pre-registered `subjects`.
pub fn overall(subjects: &[String], saved: &[SavedDecision]) -> Decision {
    let mut go = !subjects.is_empty();
    let mut parts = Vec::new();
    for s in subjects {
        let mine: Vec<&SavedDecision> = saved.iter().filter(|d| &d.decision.subject == s).collect();
        match mine.as_slice() {
            [d] if d.problems == 0 => {
                go &= d.decision.go;
                parts.push(format!(
                    "{s}={}",
                    if d.decision.go { "GO" } else { "NO-GO" }
                ));
            }
            [d] => {
                go = false;
                parts.push(format!("{s}=not a result ({} problems)", d.problems));
            }
            [] => {
                go = false;
                parts.push(format!("{s}=missing"));
            }
            _ => {
                go = false;
                parts.push(format!("{s}=reported {} times", mine.len()));
            }
        }
    }
    Decision {
        subject: "overall".into(),
        go,
        variants: Vec::new(),
        line: format!(
            "{ARM} decision overall: {} ({})",
            if go { "GO" } else { "NO-GO" },
            parts.join(" ")
        ),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn toks(rows: &[&[&str]]) -> Vec<Vec<Vec<u8>>> {
        rows.iter()
            .map(|r| r.iter().map(|t| t.as_bytes().to_vec()).collect())
            .collect()
    }

    /// Blocks of 2, 3 and 1 rows: "rare" is in rows 1 and 3 (block 0 row 1,
    /// block 1 row 1), "every" in every row, "solo" in the last row only.
    fn fixture() -> ObjectTokens {
        vec![
            toks(&[&["every", "a"], &["every", "rare"]]),
            toks(&[&["every"], &["every", "rare"], &["every", "b"]]),
            toks(&[&["every", "solo"]]),
        ]
    }

    /// The plausible wrong ordinal: the row within its block instead of the
    /// object-wide ordinal.
    fn row_terms_block_local(tokens: &ObjectTokens) -> BTreeMap<Vec<u8>, BTreeSet<u32>> {
        let mut out: BTreeMap<Vec<u8>, BTreeSet<u32>> = BTreeMap::new();
        for rows in tokens {
            for (r, row) in rows.iter().enumerate() {
                for t in row {
                    out.entry(t.clone()).or_default().insert(r as u32);
                }
            }
        }
        out
    }

    #[test]
    fn the_sidecar_maps_rows_back_exactly() {
        let tokens = fixture();
        let sidecar = build_sidecar(&tokens, 2, 2, 3).expect("build");
        assert_eq!(
            probe_sidecar(&sidecar, b"rare").expect("probe"),
            SidecarHit::Rows(BTreeSet::from([(0, 1), (1, 1)]))
        );
        assert_eq!(
            probe_sidecar(&sidecar, b"solo").expect("probe"),
            SidecarHit::Rows(BTreeSet::from([(2, 0)]))
        );
        assert_eq!(
            probe_sidecar(&sidecar, b"every").expect("probe"),
            SidecarHit::Blocks(BTreeSet::from([0, 1, 2]))
        );
        assert_eq!(
            probe_sidecar(&sidecar, b"zzz").expect("probe"),
            SidecarHit::Absent
        );
        assert_eq!(
            row_terms(&tokens).get(b"rare".as_slice()),
            Some(&BTreeSet::from([1, 3]))
        );
        // Block-local ordinals name rows 1 and 1, which the row map sends
        // to (0, 1) alone: the second match is lost.
        let wrong = row_terms_block_local(&tokens);
        assert_eq!(wrong.get(b"rare".as_slice()), Some(&BTreeSet::from([1])));
    }

    #[test]
    fn a_rebuilt_sidecar_leaves_no_residue_and_a_stale_one_does() {
        let before = fixture();
        // Delete block 0 row 1 ("every", "rare"); block 0 keeps one row.
        let mut after = before.clone();
        let deleted = after[0].remove(1);
        let stale = build_sidecar(&before, 2, 2, 3).expect("build");
        let rebuilt = build_sidecar(&after, 2, 2, 3).expect("build");
        assert_eq!(residue(&rebuilt, &after, &deleted).expect("residue"), 0);
        // The stale sidecar still names (0, 1) for "rare", now past block
        // 0's last row, and (1, 1) where the shifted ordinals put block 1's
        // row 1, which still holds "rare": exactly one residue row.
        assert_eq!(residue(&stale, &after, &deleted).expect("residue"), 1);
    }

    #[test]
    fn lookup_bytes_is_the_header_plus_one_term_block() {
        let tokens = fixture();
        let section = build_postings(&tokens, 2, 3).expect("postings");
        // Terms in order: a, b | every, rare | solo; one field, stride 2.
        let parsed = PostingsSection::parse(&section).expect("parse");
        assert_eq!(
            parsed.probe(COL_BODY, b"rare").expect("probe"),
            Some(vec![0, 1])
        );
        let header = {
            // version, count, column id, capped, stride, terms, blocks
            let mut h = 1 + 4 + 1 + 1 + 1 + 1 + 1;
            for first in ["a", "every", "solo"] {
                h += 1 + first.len() + 28;
            }
            h as u64
        };
        let blocks_total = section.len() as u64 - header;
        let before = lookup_bytes(&section, &[COL_BODY], b"0").expect("lookup");
        assert_eq!(before, header);
        let a = lookup_bytes(&section, &[COL_BODY], b"b").expect("lookup") - header;
        let e = lookup_bytes(&section, &[COL_BODY], b"rare").expect("lookup") - header;
        let s = lookup_bytes(&section, &[COL_BODY], b"zz").expect("lookup") - header;
        assert_eq!(a + e + s, blocks_total);
        assert!(a > 0 && e > 0 && s > 0);
        // A lookup that charged the whole section would read every block.
        assert!(
            lookup_bytes(&section, &[COL_BODY], b"rare").expect("lookup") < section.len() as u64
        );
    }

    fn inputs() -> Inputs {
        Inputs {
            objects: 2.0,
            raw_bytes: 2e9,
            blocks: 200.0,
            dirs: 2000.0,
            bloom: 20_000.0,
            rowmap: 400.0,
            body_page_mean: 5000.0,
            truth_blocks_mean: 2.0,
            fp_rate: 0.01,
            postings_lookup_mean: 3000.0,
            sidecar_lookup_mean: 1500.0,
        }
    }

    /// The plausible wrong bloom path: false positives counted once per
    /// object instead of once per non-matching block.
    fn bloom_per_object_fp(i: &Inputs) -> f64 {
        let n = EXTRAPOLATE_RAW_BYTES / (i.raw_bytes / i.objects);
        n * (i.dirs / i.objects + i.bloom / i.objects)
            + (i.truth_blocks_mean + i.fp_rate * n) * i.body_page_mean
    }

    #[test]
    fn extrapolation_is_the_registered_formula() {
        // N = 100e9 / 1e9 = 100 objects, B = 100, D = 1000, L = 10000,
        // R = 200, P = 5000, T = 2, f = 0.01.
        // bloom    = 100*(1000 + 10000) + (2 + 0.01*(100*100 - 2))*5000
        //          = 1_100_000 + (2 + 99.98)*5000 = 1_609_900
        // postings = 100*(1000 + 3000) + 2*5000 = 410_000
        // sidecar  = 100*1500 + 2*(1000 + 200 + 5000) = 162_400
        let (n, bloom, postings, sidecar) = extrapolate(&inputs());
        assert_eq!(n, 100.0);
        assert!((bloom - 1_609_900.0).abs() < 1e-6, "{bloom}");
        assert_eq!(postings, 410_000.0);
        assert_eq!(sidecar, 162_400.0);
        assert!((bloom_per_object_fp(&inputs()) - 1_609_900.0).abs() > 1.0);
    }

    #[test]
    fn figures_pool_fp_over_negative_blocks() {
        let q = |truth, survivors| QueryMeasure {
            truth_blocks: truth,
            survivors,
            postings_lookup: 100,
            sidecar_lookup: 50,
        };
        let o = |blocks, queries| ObjectMeasure {
            lines: 10,
            raw_bytes: 1000,
            bytes: 400,
            blocks,
            dirs: 10,
            bloom: 20,
            postings: 100,
            sidecar: 110,
            rowmap: 5,
            body_page_bytes: 40 * blocks,
            compaction_cpu_s: 0.002,
            build_cpu_s: 0.001,
            rebuild_cpu_s: 0.001,
            residue_rows: 0,
            queries,
        };
        // fp = (3-1) + (1-1) + (2-2) = 2 over (10-1) + (10-1) + (4-2) = 20.
        let objs = [o(10, vec![q(1, 3), q(1, 1)]), o(4, vec![q(2, 2)])];
        let f = figures("s", &objs);
        assert_eq!(value(&f, "s/bloom/fp_rate"), Some(0.1));
        // A per-query mean of rates would give (2/9 + 0 + 0) / 3.
        assert_ne!(value(&f, "s/bloom/fp_rate"), Some(2.0 / 9.0 / 3.0));
        assert_eq!(value(&f, "s/truth_blocks_mean"), Some(4.0 / 3.0));
        assert_eq!(value(&f, "s/index_ratio"), Some(1.1));
        assert_eq!(value(&f, "s/rewrite_amp"), Some(1020.0 / 800.0));
        assert_eq!(value(&f, "s/build_cpu_ratio"), Some(0.5));
        assert_eq!(value(&f, "s/bytes/body_page_mean"), Some(40.0));
        assert_eq!(f.len(), figure_names().len());
    }

    fn fig(subject: &str, pairs: &[(&str, f64)]) -> Vec<Figure> {
        pairs
            .iter()
            .map(|(k, v)| Figure {
                key: format!("{subject}/{k}"),
                value: *v,
            })
            .collect()
    }

    #[test]
    fn decide_edges_are_inclusive_and_residue_must_be_zero() {
        let at = |rare: f64, index: f64, build: f64, residue: f64| {
            decide(
                "s",
                &fig(
                    "s",
                    &[
                        ("rare_bytes_ratio", rare),
                        ("index_ratio", index),
                        ("build_cpu_ratio", build),
                        ("deletion/rebuild_cpu_ratio", 0.7),
                        ("deletion/residue_rows", residue),
                    ],
                ),
            )
            .go
        };
        assert!(at(0.1, 1.2, 1.5, 0.0));
        assert!(!at(0.1001, 1.2, 1.5, 0.0));
        assert!(!at(0.1, 1.2001, 1.5, 0.0));
        assert!(!at(0.1, 1.2, 1.5001, 0.0));
        assert!(!at(0.1, 1.2, 1.5, 1.0));
        let uncosted = decide("s", &fig("s", &[("rare_bytes_ratio", 0.01)]));
        assert!(!uncosted.go);
        assert!(
            uncosted.line.contains("deletion/rebuild_cpu_ratio missing"),
            "{}",
            uncosted.line
        );
    }

    #[test]
    fn overall_needs_every_subject_go_once_without_problems() {
        let saved = |s: &str, go: bool, problems: usize| SavedDecision {
            decision: Decision {
                subject: s.into(),
                go,
                variants: Vec::new(),
                line: String::new(),
            },
            problems,
            misses: 0,
        };
        let names = vec!["a".to_string(), "b".to_string()];
        assert!(overall(&names, &[saved("a", true, 0), saved("b", true, 0)]).go);
        assert!(!overall(&names, &[saved("a", true, 0), saved("b", false, 0)]).go);
        assert!(!overall(&names, &[saved("a", true, 0), saved("b", true, 1)]).go);
        assert!(!overall(&names, &[saved("a", true, 0)]).go);
        assert!(!overall(&[], &[]).go);
    }
}
