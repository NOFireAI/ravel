//! Real-RLOG orchestration for the string fingerprint experiment (issue
//! #3710, ADR-3692 work package 4, Plan row T12).
//!
//! [`crate::string_fingerprint`] is pure logic over in-memory block values and
//! predicate inputs; it never touches an RLOG object. This module is the
//! bridge: it writes one subject's corpus values as a declared `Str`
//! attribute column (never the record body, so the production `Equals` and
//! `Contains` baselines both resolve through the same named `FIELD_DIR`
//! column uniformly across every subject -- see
//! `string_fingerprint`'s module doc), reads every block back through the
//! production [`RlogReader`], measures the two real production baselines
//! (bloom via `Predicate::Equals{field: FieldSel::Attr(column)}` and
//! `ScanStats::blocks_after_bloom`; contains via
//! `Predicate::Contains{key: column}` and `ScanStats::contains_cells_tested`),
//! and feeds the result into [`crate::string_fingerprint::measure`].
//!
//! Mirrors `logbody_report`'s object/block loop (same batching into multiple
//! RLOG objects of `object_records` rows, same byte-identical read-back
//! check), adapted from the record body to a declared attribute column.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use ravel_logseg::{
    AttrValue, ColumnSelection, FieldSel, FieldType, LogRecord, ObjectIdentity, Predicate,
    RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_types::logstream::log_stream_id;

use crate::string_fingerprint::spec::{self, Kind, SubjectSpec};
use crate::string_fingerprint::{self, BlockValues, PredicateInput, SubjectInput, SubjectReport};

/// Corpus values for one subject, read from whatever `--source` named.
pub enum Corpus {
    /// One value per line of a (possibly gzip-compressed) text file --
    /// the 4 LogHub body subjects.
    Lines(PathBuf),
    /// One value per row of a named `Utf8` column of a Parquet file --
    /// the 4 ClickBench subjects. Gated behind the `parquet-baseline`
    /// feature, which already links the Parquet reader this crate uses
    /// elsewhere (`columnar_load.rs`).
    #[cfg(feature = "parquet-baseline")]
    ParquetColumn { path: PathBuf, column: String },
}

/// What one real run needs beyond the pre-registration file.
pub struct Config {
    pub spec_path: PathBuf,
    pub subject: String,
    pub corpus: Corpus,
    pub object_records: usize,
    pub zstd_level: i32,
    /// Read at most this many corpus values (default: the whole corpus).
    pub max_lines: Option<u64>,
}

fn fail(msg: impl Into<String>) -> String {
    msg.into()
}

fn open_text(path: &Path) -> Result<Box<dyn BufRead>, String> {
    let file = File::open(path).map_err(|e| fail(format!("open {}: {e}", path.display())))?;
    if path.extension().is_some_and(|x| x == "gz") {
        Ok(Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(
            file,
        ))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn next_line(input: &mut dyn BufRead, buf: &mut Vec<u8>) -> Result<Option<()>, String> {
    buf.clear();
    let n = input
        .read_until(b'\n', buf)
        .map_err(|e| fail(format!("read corpus: {e}")))?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    Ok(Some(()))
}

/// Reads every value of the named corpus, in order, up to `max_lines` when
/// given. Each value is a declared `Str` attribute's raw bytes, never a
/// record body.
fn read_values(corpus: &Corpus, max_lines: Option<u64>) -> Result<Vec<Vec<u8>>, String> {
    let cap = max_lines.unwrap_or(u64::MAX);
    match corpus {
        Corpus::Lines(path) => {
            let mut input = open_text(path)?;
            let mut buf = Vec::new();
            let mut out = Vec::new();
            while (out.len() as u64) < cap && next_line(input.as_mut(), &mut buf)?.is_some() {
                out.push(buf.clone());
            }
            Ok(out)
        }
        #[cfg(feature = "parquet-baseline")]
        Corpus::ParquetColumn { path, column } => read_parquet_column(path, column, cap),
    }
}

#[cfg(feature = "parquet-baseline")]
fn read_parquet_column(path: &Path, column: &str, cap: u64) -> Result<Vec<Vec<u8>>, String> {
    use arrow::array::{Array, StringArray};
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path).map_err(|e| fail(format!("open {}: {e}", path.display())))?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| fail(format!("open parquet {}: {e}", path.display())))?;

    let arrow_schema = builder.schema().clone();
    let field_index = arrow_schema
        .fields()
        .iter()
        .position(|f| f.name() == column)
        .ok_or_else(|| fail(format!("parquet file has no column {column:?}")))?;
    let schema_descr = builder.parquet_schema().clone();
    builder = builder.with_projection(ProjectionMask::roots(&schema_descr, [field_index]));

    let reader = builder
        .build()
        .map_err(|e| fail(format!("build parquet reader {}: {e}", path.display())))?;
    let mut out = Vec::new();
    for batch in reader {
        if out.len() as u64 >= cap {
            break;
        }
        let batch = batch.map_err(|e| fail(format!("read parquet batch: {e}")))?;
        let col = batch
            .column_by_name(column)
            .ok_or_else(|| fail(format!("parquet file has no column {column:?}")))?;
        let strings = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| fail(format!("column {column:?} is not Utf8")))?;
        for i in 0..strings.len() {
            if out.len() as u64 >= cap {
                break;
            }
            if strings.is_null(i) {
                out.push(Vec::new());
            } else {
                out.push(strings.value(i).as_bytes().to_vec());
            }
        }
    }
    Ok(out)
}

/// Refuses a run whose own parameters do not match the subject's
/// pre-registration, before any corpus byte is read (the dispatch's
/// refuse-before-reading-corpus requirement).
fn refuse_param_drift(spec: &SubjectSpec, cfg: &Config) -> Result<(), String> {
    if cfg.object_records == 0 {
        return Err(fail("object_records must be nonzero"));
    }
    // block_records, gram_len, df_threshold and budget_share are read
    // straight from the pre-registration (there is no separate CLI copy to
    // drift from it); this function exists as the single place a future CLI
    // flag for any of them must be checked against `spec` before `read_values`
    // runs.
    let _ = spec;
    Ok(())
}

struct ObjectOutcome {
    object_bytes: u64,
    blocks: Vec<BlockValues>,
    /// Per seen+unseen predicate id, accumulated across every object:
    /// (bloom_blocks_kept sum, contains_cells_tested sum).
    bloom_kept: Vec<(String, u64)>,
    contains_tested: Vec<(String, u64)>,
}

/// Writes `values` (from `first` on) as one RLOG object under `column`, reads
/// every block back, and measures the two production baselines for every
/// predicate of `spec`.
fn object(
    rlog: RlogConfig,
    writer_seq: u64,
    spec: &SubjectSpec,
    values: &[Vec<u8>],
) -> Result<ObjectOutcome, String> {
    let identity = ObjectIdentity {
        tenant_hash: [0x53; 16],
        shard: 0,
        writer_id: [0x46; 16],
        writer_epoch: 1,
        writer_seq,
    };
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("string-fingerprint-report".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "string_fingerprint_report", "1", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "string_fingerprint_report", "1", &[]);
    let mut writer = RlogWriter::new(rlog, identity);
    for (i, value) in values.iter().enumerate() {
        let text = String::from_utf8_lossy(value).into_owned();
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: i as i64,
                observed_ts_ns: i as i64,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: String::new(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![(spec.column.clone(), AttrValue::Str(text))],
            })
            .map_err(|e| fail(format!("push: {e}")))?;
    }
    let bytes = writer.finish().map_err(|e| fail(format!("finish: {e}")))?;
    let object_bytes = bytes.len() as u64;

    let reader = RlogReader::new(&bytes, &rlog).map_err(|e| fail(format!("open object: {e}")))?;

    let column_sel = ColumnSelection::fixed_only().with_attr(&spec.column);
    let mut scan = reader
        .scan_blocks(&Predicate::And(Vec::new()), &[], &column_sel)
        .map_err(|e| fail(format!("scan: {e}")))?;
    let mut blocks = Vec::new();
    let mut next = 0usize;
    while let Some(index) = scan.next_block_index() {
        let Some(view) = scan
            .next_block_columnar(bytes.as_slice())
            .map_err(|e| fail(format!("decode block {index}: {e}")))?
        else {
            break;
        };
        let attr_col = view
            .resolve_attr(&spec.column, FieldType::Str)
            .ok_or_else(|| fail(format!("block {index}: no Str column for {}", spec.column)))?;
        let n = view.surviving_count();
        let mut block_values: BlockValues = Vec::with_capacity(n);
        for i in 0..n {
            let got = view
                .bytes_at(attr_col.column_id, i)
                .ok_or_else(|| fail(format!("block {index} row {i}: column not decoded")))?;
            if values.get(next + i).map(Vec::as_slice) != Some(got) {
                return Err(fail(format!(
                    "block {index} row {i}: attribute read back differs from corpus value {}",
                    next + i
                )));
            }
            block_values.push(got.to_vec());
        }
        next += n;
        blocks.push(block_values);
    }
    if next != values.len() {
        return Err(fail(format!(
            "object read back {next} records of {}",
            values.len()
        )));
    }

    let mut bloom_kept = Vec::new();
    let mut contains_tested = Vec::new();
    for p in spec.seen.iter().chain(spec.unseen.iter()) {
        match p.kind {
            Kind::Equals => {
                let scan = reader
                    .scan_blocks(
                        &Predicate::Equals {
                            field: FieldSel::Attr(spec.column.clone()),
                            value: AttrValue::Str(p.literal.clone()),
                        },
                        &[],
                        &ColumnSelection::fixed_only(),
                    )
                    .map_err(|e| fail(format!("equals scan {}: {e}", p.id)))?;
                let stats = scan.stats();
                if stats.bloom_degraded {
                    return Err(fail(format!(
                        "BLOOM section degraded on a freshly written object (predicate {})",
                        p.id
                    )));
                }
                bloom_kept.push((p.id.clone(), u64::from(stats.blocks_after_bloom)));
            }
            Kind::Like => {
                let scan = reader
                    .scan_blocks(
                        &Predicate::Contains {
                            key: spec.column.clone(),
                            needle: p.literal.clone(),
                            negated: false,
                        },
                        &[],
                        &ColumnSelection::fixed_only(),
                    )
                    .map_err(|e| fail(format!("contains scan {}: {e}", p.id)))?;
                let stats = scan.stats();
                contains_tested.push((p.id.clone(), stats.contains_cells_tested));
            }
        }
    }

    Ok(ObjectOutcome {
        object_bytes,
        blocks,
        bloom_kept,
        contains_tested,
    })
}

/// A subject's [`SubjectReport`] plus the write-side provenance the report
/// itself carries no field for (how many RLOG objects the corpus took, and
/// their total stored size).
#[derive(Debug)]
pub struct RunReport {
    pub report: SubjectReport,
    pub objects: u64,
    pub object_bytes: u64,
}

/// Reads the subject's corpus, writes and reads it back through a real RLOG
/// object per `object_records` values, measures the two production baselines,
/// and runs [`string_fingerprint::measure`] on the result.
pub fn run(cfg: &Config) -> Result<RunReport, String> {
    let text = std::fs::read_to_string(&cfg.spec_path)
        .map_err(|e| fail(format!("read {}: {e}", cfg.spec_path.display())))?;
    let parsed = spec::parse(&text)?;
    let subject = parsed.subject(&cfg.subject).ok_or_else(|| {
        fail(format!(
            "no subject named {:?} in {}",
            cfg.subject,
            cfg.spec_path.display()
        ))
    })?;
    refuse_param_drift(subject, cfg)?;

    let values = read_values(&cfg.corpus, cfg.max_lines)?;
    let rlog = RlogConfig {
        block_target_records: subject.block_records,
        zstd_level: cfg.zstd_level,
        ..RlogConfig::default()
    };

    let mut blocks: Vec<BlockValues> = Vec::new();
    let mut compressed_bytes = 0u64;
    let mut bloom_kept_by_id: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    let mut contains_tested_by_id: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    let mut writer_seq = 0u64;
    for chunk in values.chunks(cfg.object_records.max(1)) {
        let outcome = object(rlog, writer_seq, subject, chunk)?;
        writer_seq += 1;
        compressed_bytes += outcome.object_bytes;
        blocks.extend(outcome.blocks);
        for (id, kept) in outcome.bloom_kept {
            *bloom_kept_by_id.entry(id).or_default() += kept;
        }
        for (id, tested) in outcome.contains_tested {
            *contains_tested_by_id.entry(id).or_default() += tested;
        }
    }

    let build_input = |preds: &[spec::PredicateSpec]| -> Vec<PredicateInput> {
        preds
            .iter()
            .map(|p| PredicateInput {
                id: p.id.clone(),
                kind: p.kind,
                literal: p.literal.as_bytes().to_vec(),
                bloom_blocks_kept: bloom_kept_by_id.get(&p.id).copied(),
                contains_cells_tested: contains_tested_by_id.get(&p.id).copied(),
            })
            .collect()
    };
    let seen = build_input(&subject.seen);
    let unseen = build_input(&subject.unseen);

    let input = SubjectInput {
        lines: values.len() as u64,
        blocks: &blocks,
        zstd_level: cfg.zstd_level,
        seen: &seen,
        unseen: &unseen,
    };
    let report = string_fingerprint::measure(subject, &input)?;
    Ok(RunReport {
        report,
        objects: writer_seq,
        object_bytes: compressed_bytes,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/string_fingerprint")
            .join(name)
    }

    fn fixture_cfg(subject: &str, object_records: usize) -> Config {
        Config {
            spec_path: fixture("test_subjects.toml"),
            subject: subject.to_string(),
            corpus: Corpus::Lines(fixture("corpus.txt")),
            object_records,
            zstd_level: 3,
            max_lines: None,
        }
    }

    #[test]
    fn run_round_trips_a_small_corpus_through_a_real_rlog_object() {
        let run_report = run(&fixture_cfg("test_subject", 4))
            .expect("run should succeed on a well-formed fixture");
        assert_eq!(run_report.objects, 1);
        assert!(run_report.object_bytes > 0);
        let by_key: BTreeMap<&str, f64> = run_report
            .report
            .figures
            .iter()
            .map(|f| (f.key.as_str(), f.value))
            .collect();
        assert_eq!(by_key["test_subject/lines"], 4.0);
        // block_records = 2 over 4 corpus lines: two full blocks.
        assert_eq!(by_key["test_subject/blocks"], 2.0);
        assert!(
            run_report.report.problems.is_empty(),
            "{:?}",
            run_report.report.problems
        );
    }

    #[test]
    fn run_splits_one_corpus_across_multiple_rlog_objects() {
        // object_records=2 forces two objects of two lines each, exercising
        // the per-object accumulation (bloom_kept/contains_tested summed
        // across objects, blocks concatenated in corpus order).
        let run_report = run(&fixture_cfg("test_subject", 2))
            .expect("run should succeed across multiple objects");
        assert_eq!(run_report.objects, 2);
        assert!(
            run_report.report.problems.is_empty(),
            "{:?}",
            run_report.report.problems
        );
    }

    #[test]
    fn run_refuses_zero_object_records() {
        let err =
            run(&fixture_cfg("test_subject", 0)).expect_err("object_records=0 must be refused");
        assert!(err.contains("object_records"), "{err}");
    }

    #[test]
    fn run_refuses_an_unknown_subject() {
        let err = run(&fixture_cfg("nope", 4)).expect_err("unknown subject must be refused");
        assert!(err.contains("nope"), "{err}");
    }
}
