//! The measurement half of [`crate::fulltext_sidecar`]: writes each object
//! with the production RLOG writer, reads its real BLOOM, directories and
//! body pages, and measures the sidecar and the body postings on the same
//! objects.

use std::collections::{BTreeMap, BTreeSet};

use ravel_logseg::footer::{self, kind};
use ravel_logseg::postings::PostingsSection;
use ravel_logseg::record::COL_BODY;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::{ColumnSelection, FieldSel, LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_types::TenantId;

use crate::fulltext_sidecar::{
    BLOCK_LEVEL_FIELD, ObjectMeasure, ObjectTokens, QueryMeasure, SidecarHit, SubjectSpec,
    build_postings, build_sidecar, figures, lookup_bytes, probe_sidecar, residue, row_terms,
};
use crate::structural_read_report::real_page;
use crate::t13::{
    Decision, Expect, Figure, Outcome, RowMap, SavedDecision, SplitMix, cpu_time, decode_all,
    identity, line_records, median, write_rlog,
};

/// Every row's body tokens, per block.
pub fn object_tokens(blocks: &[Vec<LogRecord>]) -> ObjectTokens {
    blocks
        .iter()
        .map(|b| b.iter().map(|r| tokens(&r.body)).collect())
        .collect()
}

/// The sidecar build as the CPU figure times it: body decode, tokenize,
/// encode.
fn build(
    bytes: &[u8],
    spec: &SubjectSpec,
) -> Result<(ObjectTokens, crate::fulltext_sidecar::Sidecar), String> {
    let blocks = decode_all(bytes, &ColumnSelection::fixed_only().with_body())?;
    let toks = object_tokens(&blocks);
    let sidecar = build_sidecar(&toks, spec.row_level_max_rows, spec.stride, spec.zstd_level)?;
    Ok((toks, sidecar))
}

/// Median CPU seconds of `reps` runs of `f`, and the last run's value.
fn timed<T>(reps: usize, mut f: impl FnMut() -> Result<T, String>) -> Result<(T, f64), String> {
    let mut times = Vec::with_capacity(reps);
    let mut last = None;
    for _ in 0..reps {
        let (v, s) = cpu_time(&mut f);
        last = Some(v?);
        times.push(s);
    }
    let v = last.ok_or("reps must be nonzero")?;
    Ok((v, median(&times).unwrap_or(0.0)))
}

/// The blocks a `HasWord` body scan of the public reader leaves after its
/// skip-index and bloom pruning, without decoding any of them.
fn bloom_survivors(bytes: &[u8], word: &str) -> Result<BTreeSet<u32>, String> {
    let reader =
        RlogReader::new(bytes, &RlogConfig::default()).map_err(|e| format!("open: {e}"))?;
    let pred = Predicate::HasWord {
        field: FieldSel::Body,
        word: word.to_string(),
    };
    let mut scan = reader
        .scan_blocks(&pred, &[], &ColumnSelection::fixed_only())
        .map_err(|e| format!("scan: {e}"))?;
    let mut out = BTreeSet::new();
    while let Some(b) = scan.next_block_index() {
        out.insert(b as u32);
        scan.skip_next_block();
    }
    Ok(out)
}

/// Directory bytes a reader fetches before any page: the stream, field,
/// skip, page and key directories and everything after the last section
/// (the footer and trailer).
fn dir_bytes(bytes: &[u8]) -> Result<u64, String> {
    let foot = footer::open(bytes).map_err(|e| format!("footer: {e}"))?;
    let mut dirs = 0u64;
    let mut end = 0u64;
    for k in [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::BLOCKS,
        kind::SKIP_IDX,
        kind::BLOOM,
        kind::POSTINGS,
        kind::PAGE_DIR,
        kind::KEY_IDX,
    ] {
        if let Some(s) = foot.section(k) {
            end = end.max(s.offset + s.len);
            if ![kind::BLOCKS, kind::BLOOM, kind::POSTINGS].contains(&k) {
                dirs += s.len;
            }
        }
    }
    Ok(dirs + (bytes.len() as u64).saturating_sub(end))
}

/// One object's measurement; `problems` collects every verdict that
/// disagrees with the truth.
fn measure_object(
    spec: &SubjectSpec,
    index: usize,
    lines: &[Vec<u8>],
    problems: &mut Vec<String>,
) -> Result<ObjectMeasure, String> {
    let th = TenantId::new(format!("t13-fulltext-{}", spec.name)).hash();
    let cfg = RlogConfig {
        block_target_records: spec.block_records,
        zstd_level: spec.zstd_level,
        ..RlogConfig::default()
    };
    let first = (index * spec.lines_per_object) as u64;
    let records = line_records(lines, first);
    let seq = 2 * index as u64 + 1;
    let bytes = write_rlog(records.clone(), cfg, identity(&th, seq))?;
    let at = |m: String| format!("object {index}: {m}");

    let (_, compaction_cpu_s) = timed(spec.reps, || {
        let blocks = decode_all(&bytes, &ColumnSelection::all())?;
        write_rlog(
            blocks.into_iter().flatten().collect(),
            cfg,
            identity(&th, seq),
        )
    })?;
    let ((toks, sidecar), build_cpu_s) = timed(spec.reps, || build(&bytes, spec))?;
    let postings = build_postings(&toks, spec.stride, spec.zstd_level)?;
    let parsed = PostingsSection::parse(&postings).map_err(|e| at(format!("postings: {e}")))?;

    let foot = footer::open(&bytes).map_err(|e| at(format!("footer: {e}")))?;
    let bloom = foot.section(kind::BLOOM).map(|s| s.len);
    if bloom.is_none() {
        problems.push(at("the production writer wrote no BLOOM section".into()));
    }
    let blocks_off = foot
        .section(kind::BLOCKS)
        .map(|s| s.offset)
        .ok_or_else(|| at("no BLOCKS section".into()))?;
    let dirs = RlogReader::<[u8]>::decode_directories(&bytes, &RlogConfig::default())
        .map_err(|e| at(format!("directories: {e}")))?;
    let page = |b: u32| {
        let (data, dict) = real_page(dirs.page_dir(), blocks_off, b, COL_BODY);
        data.map_or(0, |d| d.1) + dict.map_or(0, |d| d.1)
    };
    let nblocks = toks.len() as u32;
    let body_page_bytes: u64 = (0..nblocks).map(page).sum();

    let counts: Vec<u32> = toks.iter().map(|b| b.len() as u32).collect();
    let map = RowMap::new(&counts);
    let rows = row_terms(&toks);
    let rare: Vec<(&Vec<u8>, &BTreeSet<u32>)> = rows
        .iter()
        .filter(|(_, o)| (spec.rare_min_rows..=spec.rare_max_rows).contains(&o.len()))
        .collect();
    if rare.len() < spec.queries_per_object {
        problems.push(at(format!(
            "{} rare tokens, fewer than queries_per_object {}",
            rare.len(),
            spec.queries_per_object
        )));
    }
    let k = spec.queries_per_object.min(rare.len());
    let mut queries = Vec::with_capacity(k);
    for pick in SplitMix::new(spec.seed ^ index as u64).distinct(k, rare.len() as u64) {
        let (token, ordinals) = rare[pick as usize];
        let word = String::from_utf8(token.clone()).map_err(|e| at(format!("token: {e}")))?;
        let truth_rows: BTreeSet<(u32, u32)> = ordinals
            .iter()
            .filter_map(|&o| map.locate(u64::from(o)))
            .collect();
        let truth: BTreeSet<u32> = truth_rows.iter().map(|(b, _)| *b).collect();
        let survivors = bloom_survivors(&bytes, &word).map_err(at)?;
        if !truth.is_subset(&survivors) {
            problems.push(at(format!("bloom pruned a block holding {word:?}")));
        }
        let probed: BTreeSet<u32> = parsed
            .probe(COL_BODY, token)
            .map_err(|e| at(format!("postings probe: {e}")))?
            .unwrap_or_default()
            .into_iter()
            .collect();
        if probed != truth {
            problems.push(at(format!(
                "postings named {probed:?} for {word:?}, truth {truth:?}"
            )));
        }
        let hit = probe_sidecar(&sidecar, token).map_err(at)?;
        if hit != SidecarHit::Rows(truth_rows.clone()) {
            problems.push(at(format!(
                "sidecar named {hit:?} for {word:?}, truth {truth_rows:?}"
            )));
        }
        queries.push(QueryMeasure {
            truth_blocks: truth.len() as u64,
            survivors: survivors.len() as u64,
            postings_lookup: lookup_bytes(&postings, &[COL_BODY], token).map_err(at)?,
            sidecar_lookup: lookup_bytes(&sidecar.section, &[COL_BODY, BLOCK_LEVEL_FIELD], token)
                .map_err(at)?,
        });
    }

    // Deletion: one seeded row out, the object rewritten, the sidecar rebuilt.
    let gone =
        SplitMix::new(spec.seed ^ 0xde1e_7e00 ^ index as u64).below(records.len() as u64) as usize;
    let mut kept = records.clone();
    let deleted_record = kept.remove(gone);
    let rewritten = write_rlog(kept, cfg, identity(&th, seq + 1))?;
    let ((after, rebuilt), rebuild_cpu_s) = timed(spec.reps, || build(&rewritten, spec))?;
    let residue_rows = residue(&rebuilt, &after, &tokens(&deleted_record.body)).map_err(at)?;

    Ok(ObjectMeasure {
        lines: lines.len() as u64,
        raw_bytes: lines.iter().map(|l| l.len() as u64).sum(),
        bytes: bytes.len() as u64,
        blocks: u64::from(nblocks),
        dirs: dir_bytes(&bytes).map_err(at)?,
        bloom: bloom.unwrap_or(0),
        postings: postings.len() as u64,
        sidecar: sidecar.len(),
        rowmap: sidecar.rowmap.len() as u64,
        body_page_bytes,
        compaction_cpu_s,
        build_cpu_s,
        rebuild_cpu_s,
        residue_rows,
        queries,
    })
}

pub fn run_subject(spec: &SubjectSpec, lines: &[Vec<u8>]) -> Result<Outcome, String> {
    let need = spec.objects * spec.lines_per_object;
    if lines.len() < need {
        return Err(format!(
            "subject {}: the corpus holds {} lines, {} objects of {} need {need}",
            spec.name,
            lines.len(),
            spec.objects,
            spec.lines_per_object
        ));
    }
    let mut problems = Vec::new();
    let mut objs = Vec::with_capacity(spec.objects);
    for (i, chunk) in lines[..need].chunks(spec.lines_per_object).enumerate() {
        objs.push(measure_object(spec, i, chunk, &mut problems)?);
    }
    let notes = vec![
        format!(
            "objects {} of {} lines: bytes {:?}, blocks {:?}, queries {:?}",
            spec.objects,
            spec.lines_per_object,
            objs.iter().map(|o| o.bytes).collect::<Vec<_>>(),
            objs.iter().map(|o| o.blocks).collect::<Vec<_>>(),
            objs.iter().map(|o| o.queries.len()).collect::<Vec<_>>()
        ),
        "bloom path charges each object's whole BLOOM section; a body page is its data \
         extent plus its chunk's dictionary page when it has one"
            .into(),
        crate::fulltext_sidecar::FORMULA.into(),
    ];
    Ok(Outcome {
        figures: figures(&spec.name, &objs),
        problems,
        notes,
    })
}

/// Arm 2 as the report bin drives it.
pub struct FulltextSidecar;

impl crate::t13::Arm for FulltextSidecar {
    const NAME: &'static str = crate::fulltext_sidecar::ARM;
    type Subject = SubjectSpec;

    fn subjects(spec_text: &str) -> Result<Vec<SubjectSpec>, String> {
        Ok(crate::fulltext_sidecar::parse(spec_text)?.subject)
    }
    fn name(s: &SubjectSpec) -> &str {
        &s.name
    }
    fn corpus_sha256(s: &SubjectSpec) -> &str {
        &s.corpus_sha256
    }
    fn max_lines(s: &SubjectSpec) -> u64 {
        s.max_lines
    }
    fn expectations(s: &SubjectSpec) -> BTreeMap<String, Expect> {
        s.expectations()
    }
    fn measure(s: &SubjectSpec, lines: &[Vec<u8>]) -> Result<Outcome, String> {
        run_subject(s, lines)
    }
    fn decide(subject: &str, figures: &[Figure]) -> Decision {
        crate::fulltext_sidecar::decide(subject, figures)
    }
    fn overall(subjects: &[String], saved: &[SavedDecision]) -> Decision {
        crate::fulltext_sidecar::overall(subjects, saved)
    }
}
