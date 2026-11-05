//! Real-RLOG orchestration for the string fingerprint experiment (issue
//! #3710, ADR-3692 work package 4, Plan row T12).
//!
//! [`crate::string_fingerprint`] is the measurement over in-memory block
//! values; this module is the bridge to production code. It writes one
//! subject's corpus values as a declared `Str` attribute column (never the
//! record body) into RLOG objects of `object_records` rows, reads every block
//! back through [`RlogReader`] and checks it byte for byte, draws the
//! workload from the read-back blocks, and answers the production baselines
//! ([`Objects`], a [`Production`]) over the same objects.
//!
//! The writer is built without indexed fields, so the objects carry no
//! POSTINGS section and equality pruning is the skip index plus the bloom.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use ravel_logseg::{
    AttrValue, ColumnSelection, FieldSel, FieldType, LogRecord, ObjectIdentity, Predicate,
    RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_types::logstream::log_stream_id;

use crate::string_fingerprint::spec::{self, SubjectSpec};
use crate::string_fingerprint::{
    self, BlockValues, CPU_CLOCK, ContainsRun, Production, SATURATION_BYTES, Stamp, SubjectInput,
    SubjectReport, block_texts, workload,
};

/// Corpus values for one subject.
pub enum Corpus {
    /// One value per line of a (possibly gzip-compressed) text file.
    Lines(PathBuf),
    /// One value per row of a named `Utf8` or `Binary` column of a Parquet file (a null
    /// reads as the empty string). Behind the `parquet-baseline` feature,
    /// which links the Parquet reader.
    #[cfg(feature = "parquet-baseline")]
    ParquetColumn { path: PathBuf, column: String },
}

impl Corpus {
    fn path(&self) -> &Path {
        match self {
            Corpus::Lines(p) => p,
            #[cfg(feature = "parquet-baseline")]
            Corpus::ParquetColumn { path, .. } => path,
        }
    }
}

/// Run parameters given on the command line. Each one given must equal the
/// subject's pre-registered value; the run itself always uses the
/// pre-registered value.
#[derive(Clone, Debug, Default)]
pub struct Overrides {
    pub gram_len: Option<usize>,
    pub budget_share: Option<f64>,
    pub fallback_min_bits: Option<usize>,
    pub zstd_level: Option<i32>,
    pub block_records: Option<usize>,
    pub object_records: Option<usize>,
    pub max_lines: Option<u64>,
    pub seed: Option<u64>,
}

/// What one real run needs.
pub struct Config {
    pub spec_path: PathBuf,
    pub subject: String,
    pub corpus: Corpus,
    pub overrides: Overrides,
}

fn open_text(path: &Path) -> Result<Box<dyn BufRead>, String> {
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
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
        .map_err(|e| format!("read corpus: {e}"))?;
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

/// Reads the corpus's values in order, at most `cap` of them.
pub fn read_values(corpus: &Corpus, cap: u64) -> Result<Vec<Vec<u8>>, String> {
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
    use arrow::array::{Array, BinaryArray, StringArray};
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("open parquet {}: {e}", path.display()))?;
    let field_index = builder
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == column)
        .ok_or_else(|| format!("parquet file has no column {column:?}"))?;
    let schema_descr = builder.parquet_schema().clone();
    builder = builder.with_projection(ProjectionMask::roots(&schema_descr, [field_index]));
    let reader = builder
        .build()
        .map_err(|e| format!("build parquet reader {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for batch in reader {
        if out.len() as u64 >= cap {
            break;
        }
        let batch = batch.map_err(|e| format!("read parquet batch: {e}"))?;
        let col = batch
            .column_by_name(column)
            .ok_or_else(|| format!("parquet file has no column {column:?}"))?;
        let strings = col.as_any().downcast_ref::<StringArray>();
        let binary = col.as_any().downcast_ref::<BinaryArray>();
        for i in 0..col.len() {
            if out.len() as u64 >= cap {
                break;
            }
            let value: &[u8] = match (strings, binary) {
                _ if col.is_null(i) => &[],
                (Some(s), _) => s.value(i).as_bytes(),
                (None, Some(b)) => b.value(i),
                (None, None) => {
                    return Err(format!("column {column:?} is neither Utf8 nor Binary"));
                }
            };
            out.push(value.to_vec());
        }
    }
    Ok(out)
}

/// Refuses a run whose command line names a parameter value other than the
/// subject's pre-registered one. Runs before any corpus byte is read.
pub fn refuse_param_drift(spec: &SubjectSpec, o: &Overrides) -> Result<(), String> {
    let mut drift = Vec::new();
    let mut check = |name: &str, given: Option<String>, registered: String| {
        if let Some(g) = given
            && g != registered
        {
            drift.push(format!("{name}={g} (pre-registered {registered})"));
        }
    };
    check(
        "gram_len",
        o.gram_len.map(|v| v.to_string()),
        spec.gram_len.to_string(),
    );
    // Compared as bits, so 0.01 and 0.010000000000000002 differ.
    check(
        "budget_share",
        o.budget_share.map(|v| format!("{v} ({:#x})", v.to_bits())),
        format!("{} ({:#x})", spec.budget_share, spec.budget_share.to_bits()),
    );
    check(
        "fallback_min_bits",
        o.fallback_min_bits.map(|v| v.to_string()),
        spec.fallback_min_bits.to_string(),
    );
    check(
        "zstd_level",
        o.zstd_level.map(|v| v.to_string()),
        spec.zstd_level.to_string(),
    );
    check(
        "block_records",
        o.block_records.map(|v| v.to_string()),
        spec.block_records.to_string(),
    );
    check(
        "object_records",
        o.object_records.map(|v| v.to_string()),
        spec.object_records.to_string(),
    );
    check(
        "max_lines",
        o.max_lines.map(|v| v.to_string()),
        spec.max_lines.to_string(),
    );
    check("seed", o.seed.map(|v| v.to_string()), spec.seed.to_string());
    if drift.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "subject {}: refusing parameters that differ from the pre-registration: {}",
            spec.name,
            drift.join(", ")
        ))
    }
}

fn command_out(cmd: &str, args: &[&str]) -> String {
    std::process::Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        )
}

fn mem_total_kib() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| {
            m.lines()
                .find_map(|l| l.strip_prefix("MemTotal:"))
                .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        })
        .unwrap_or(0)
}

/// The corpus file's sha256 and byte count.
pub fn file_digest(path: &Path) -> Result<(String, u64), String> {
    let mut file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buf = vec![0u8; 1 << 20];
    let mut bytes = 0u64;
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        ctx.update(buf.get(..n).unwrap_or_default());
    }
    let hex: String = ctx
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok((hex, bytes))
}

pub fn stamp(corpus: &Path, zstd_level: i32) -> Result<Stamp, String> {
    let (corpus_sha256, corpus_bytes) = file_digest(corpus)?;
    let mut git = command_out("git", &["rev-parse", "HEAD"]);
    if !command_out("git", &["status", "--porcelain", "--untracked-files=no"]).is_empty() {
        git.push_str("-dirty");
    }
    Ok(Stamp {
        git,
        arch: command_out("uname", &["-m"]),
        nproc: string_fingerprint::threads(),
        mem_total_kib: mem_total_kib(),
        corpus_file: corpus
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
        corpus_sha256,
        corpus_bytes,
        zstd_level,
        cpu_clock: CPU_CLOCK.to_string(),
    })
}

/// One written object and where its blocks sit among the subject's.
struct Object {
    bytes: Vec<u8>,
    first_block: usize,
}

/// The subject's RLOG objects, answering the production baselines.
pub struct Objects {
    rlog: RlogConfig,
    column: String,
    objects: Vec<Object>,
    blocks: usize,
}

fn write_object(
    rlog: RlogConfig,
    writer_seq: u64,
    column: &str,
    values: &[Vec<u8>],
) -> Result<Vec<u8>, String> {
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
        let text = String::from_utf8(value.clone())
            .map_err(|e| format!("value {i} of object {writer_seq} is not UTF-8: {e}"))?;
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
                attrs: vec![(column.to_string(), AttrValue::Str(text))],
            })
            .map_err(|e| format!("push: {e}"))?;
    }
    writer.finish().map_err(|e| format!("finish: {e}"))
}

/// Every block of `bytes`, read back through the production reader and
/// checked against `values` byte for byte.
fn read_back(
    bytes: &[u8],
    rlog: &RlogConfig,
    column: &str,
    values: &[Vec<u8>],
) -> Result<Vec<BlockValues>, String> {
    let reader = RlogReader::new(bytes, rlog).map_err(|e| format!("open object: {e}"))?;
    let sel = ColumnSelection::fixed_only().with_attr(column);
    let mut scan = reader
        .scan_blocks(&Predicate::And(Vec::new()), &[], &sel)
        .map_err(|e| format!("scan: {e}"))?;
    let mut blocks = Vec::new();
    let mut next = 0usize;
    while let Some(index) = scan.next_block_index() {
        if index != blocks.len() {
            return Err(format!("block {index} read back out of order"));
        }
        let Some(view) = scan
            .next_block_columnar(bytes)
            .map_err(|e| format!("decode block {index}: {e}"))?
        else {
            break;
        };
        let col = view
            .resolve_attr(column, FieldType::Str)
            .ok_or_else(|| format!("block {index}: no Str column {column}"))?;
        let n = view.surviving_count();
        let mut block: BlockValues = Vec::with_capacity(n);
        for i in 0..n {
            let got = view
                .bytes_at(col.column_id, i)
                .ok_or_else(|| format!("block {index} row {i}: column not decoded"))?;
            if values.get(next + i).map(Vec::as_slice) != Some(got) {
                return Err(format!(
                    "block {index} row {i}: read back differs from corpus value {}",
                    next + i
                ));
            }
            block.push(got.to_vec());
        }
        next += n;
        blocks.push(block);
    }
    if next != values.len() {
        return Err(format!(
            "object read back {next} records of {}",
            values.len()
        ));
    }
    Ok(blocks)
}

impl Objects {
    /// Writes `values` into objects of `object_records` rows and reads every
    /// block back.
    pub fn write(
        values: &[Vec<u8>],
        column: &str,
        block_records: usize,
        object_records: usize,
        zstd_level: i32,
    ) -> Result<(Objects, Vec<BlockValues>), String> {
        if object_records == 0 || block_records == 0 {
            return Err("object_records and block_records must be nonzero".into());
        }
        let rlog = RlogConfig {
            block_target_records: block_records,
            zstd_level,
            ..RlogConfig::default()
        };
        let chunks: Vec<&[Vec<u8>]> = values.chunks(object_records).collect();
        let written = string_fingerprint::par_map(&chunks, |chunk| {
            let seq = chunks
                .iter()
                .position(|c| std::ptr::eq(*c, *chunk))
                .unwrap_or(0) as u64;
            let bytes = write_object(rlog, seq, column, chunk)?;
            let blocks = read_back(&bytes, &rlog, column, chunk)?;
            Ok::<_, String>((bytes, blocks))
        })?;
        let mut objects = Vec::with_capacity(written.len());
        let mut all = Vec::new();
        for w in written {
            let (bytes, blocks) = w?;
            objects.push(Object {
                bytes,
                first_block: all.len(),
            });
            all.extend(blocks);
        }
        Ok((
            Objects {
                rlog,
                column: column.to_string(),
                objects,
                blocks: all.len(),
            },
            all,
        ))
    }

    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    pub fn object_bytes(&self) -> u64 {
        self.objects.iter().map(|o| o.bytes.len() as u64).sum()
    }

    fn reader<'a>(&self, o: &'a Object) -> Result<RlogReader<'a>, String> {
        RlogReader::new(&o.bytes, &self.rlog).map_err(|e| format!("open object: {e}"))
    }
}

impl Production for Objects {
    fn equals_blocks_kept(&self, literal: &str) -> Result<u64, String> {
        let pred = Predicate::Equals {
            field: FieldSel::Attr(self.column.clone()),
            value: AttrValue::Str(literal.to_string()),
        };
        let mut kept = 0u64;
        for o in &self.objects {
            let scan = self
                .reader(o)?
                .scan_blocks(&pred, &[], &ColumnSelection::fixed_only())
                .map_err(|e| format!("equals scan: {e}"))?;
            let stats = scan.stats();
            if stats.bloom_degraded {
                return Err("BLOOM section degraded on a freshly written object".into());
            }
            kept += u64::from(stats.blocks_after_bloom);
        }
        Ok(kept)
    }

    fn contains(&self, literal: &str, keep: Option<&[bool]>) -> Result<ContainsRun, String> {
        let pred = Predicate::Contains {
            key: self.column.clone(),
            needle: literal.to_string(),
            negated: false,
        };
        let sel = ColumnSelection::fixed_only().with_attr(&self.column);
        let mut rows = vec![0u64; self.blocks];
        let mut cells_tested = 0u64;
        for o in &self.objects {
            let reader = self.reader(o)?;
            let mut scan = reader
                .scan_blocks(&pred, &[], &sel)
                .map_err(|e| format!("contains scan: {e}"))?;
            if let Some(keep) = keep {
                let ordinals: Vec<usize> = scan
                    .survivor_block_indices()
                    .iter()
                    .enumerate()
                    .filter(|&(_, &b)| keep.get(o.first_block + b).copied().unwrap_or(true))
                    .map(|(i, _)| i)
                    .collect();
                scan = reader
                    .scan_blocks_subset(&pred, &[], &sel, &ordinals)
                    .map_err(|e| format!("contains subset scan: {e}"))?;
            }
            while let Some(index) = scan.next_block_index() {
                let Some(view) = scan
                    .next_block_columnar(o.bytes.as_slice())
                    .map_err(|e| format!("contains decode block {index}: {e}"))?
                else {
                    break;
                };
                let n = view.surviving_count() as u64;
                let slot = rows
                    .get_mut(o.first_block + index)
                    .ok_or_else(|| format!("block {index} past the subject's blocks"))?;
                *slot += n;
            }
            cells_tested += scan.stats().contains_cells_tested;
        }
        Ok(ContainsRun { cells_tested, rows })
    }
}

/// A subject's report and the write-side figures it does not carry.
#[derive(Debug)]
pub struct RunReport {
    pub report: SubjectReport,
    pub objects: usize,
    pub object_bytes: u64,
}

fn load_subject(path: &Path, name: &str) -> Result<SubjectSpec, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let parsed = spec::parse(&text)?;
    parsed
        .subject(name)
        .cloned()
        .ok_or_else(|| format!("no subject named {name:?} in {}", path.display()))
}

/// Refuses drifted parameters, stamps the run, reads exactly `max_lines`
/// values, writes and reads them back through real RLOG objects, draws the
/// workload, and measures.
pub fn run(cfg: &Config) -> Result<RunReport, String> {
    let subject = load_subject(&cfg.spec_path, &cfg.subject)?;
    refuse_param_drift(&subject, &cfg.overrides)?;
    #[cfg(feature = "parquet-baseline")]
    if let Corpus::ParquetColumn { column, .. } = &cfg.corpus
        && *column != subject.column
    {
        return Err(format!(
            "subject {}: refusing --corpus-column {column:?}: the pre-registered column is {:?}",
            subject.name, subject.column
        ));
    }
    let stamp = stamp(cfg.corpus.path(), subject.zstd_level)?;

    let values = read_values(&cfg.corpus, subject.max_lines)?;
    if values.len() as u64 != subject.max_lines {
        return Err(format!(
            "corpus holds {} values, fewer than the pre-registered max_lines {}",
            values.len(),
            subject.max_lines
        ));
    }
    let (objects, blocks) = Objects::write(
        &values,
        &subject.column,
        subject.block_records,
        subject.object_records,
        subject.zstd_level,
    )?;
    drop(values);
    let texts = block_texts(&blocks)?;
    let workload = workload::draw(&blocks, &texts, &subject.workload_params())?;
    let input = SubjectInput {
        lines: subject.max_lines,
        blocks: &blocks,
        texts: &texts,
        workload: &workload,
    };
    let mut report = string_fingerprint::measure(&subject, &input, &objects)?;
    report.stamp = Some(stamp);
    Ok(RunReport {
        report,
        objects: objects.object_count(),
        object_bytes: objects.object_bytes(),
    })
}

/// Facts about a corpus that the pre-registration needs before any run.
pub fn describe(corpus: &Corpus, cap: u64) -> Result<String, String> {
    let values = read_values(corpus, cap)?;
    let n = values.len();
    let short = values
        .iter()
        .filter(|v| v.len() <= string_fingerprint::BLOOM_EXACT_MAX)
        .count();
    let long = values.iter().filter(|v| v.len() > SATURATION_BYTES).count();
    let empty = values.iter().filter(|v| v.is_empty()).count();
    let non_utf8 = values
        .iter()
        .filter(|v| std::str::from_utf8(v).is_err())
        .count();
    let bytes: u64 = values.iter().map(|v| v.len() as u64).sum();
    let distinct = values
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    Ok(format!(
        "string_fingerprint describe values={n} bytes={bytes} mean_len={:.1} empty={empty} \
         non_utf8={non_utf8} distinct={distinct} at_most_{}_bytes={short} over_{SATURATION_BYTES}_bytes={long}",
        if n == 0 { 0.0 } else { bytes as f64 / n as f64 },
        string_fingerprint::BLOOM_EXACT_MAX,
    ))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/string_fingerprint")
            .join(name)
    }

    fn corpus_values() -> Vec<Vec<u8>> {
        read_values(&Corpus::Lines(fixture("corpus.txt")), u64::MAX).expect("corpus")
    }

    fn cfg(subject: &str, overrides: Overrides) -> Config {
        Config {
            spec_path: fixture("test_subjects.toml"),
            subject: subject.to_string(),
            corpus: Corpus::Lines(fixture("corpus.txt")),
            overrides,
        }
    }

    #[test]
    fn bloom_prunes_an_absent_short_literal_on_real_objects() {
        let values = corpus_values();
        let (objects, blocks) = Objects::write(&values, "value", 4, 8, 3).expect("write");
        assert!(objects.object_count() >= 2);
        let present = std::str::from_utf8(&blocks[0][0]).expect("utf8");
        assert!(present.len() <= string_fingerprint::BLOOM_EXACT_MAX);
        assert!(objects.equals_blocks_kept(present).expect("eq") >= 1);
        assert_eq!(
            objects
                .equals_blocks_kept("no such value anywhere")
                .expect("eq"),
            0
        );
    }

    #[test]
    fn contains_tests_cells_and_returns_the_holders_rows() {
        let values = corpus_values();
        let (objects, blocks) = Objects::write(&values, "value", 4, 8, 3).expect("write");
        let want: Vec<u64> = blocks
            .iter()
            .map(|b| {
                b.iter()
                    .filter(|v| v.windows(6).any(|w| w == b"needle"))
                    .count() as u64
            })
            .collect();
        assert!(want.iter().filter(|&&n| n > 0).count() >= 2, "{want:?}");
        let run = objects.contains("needle", None).expect("contains");
        assert!(run.cells_tested > 0, "the Contains kernel never ran");
        assert_eq!(run.rows, want);

        let keep: Vec<bool> = want.iter().map(|&n| n > 0).collect();
        assert!(keep.contains(&false), "the summary must drop a block");
        let sub = objects.contains("needle", Some(&keep)).expect("subset");
        assert_eq!(sub.rows, want);
        assert!(sub.cells_tested > 0);
        assert!(
            sub.cells_tested < run.cells_tested,
            "dropping a block must test fewer cells: {} of {}",
            sub.cells_tested,
            run.cells_tested
        );
    }

    #[test]
    fn refuses_a_drifted_parameter_before_reading_the_corpus() {
        for o in [
            Overrides {
                budget_share: Some(0.02),
                ..Overrides::default()
            },
            Overrides {
                seed: Some(99),
                ..Overrides::default()
            },
            Overrides {
                zstd_level: Some(9),
                ..Overrides::default()
            },
        ] {
            let mut c = cfg("test_subject", o);
            c.corpus = Corpus::Lines(fixture("no-such-corpus.txt"));
            let err = run(&c).expect_err("drift must be refused");
            assert!(err.contains("refusing parameters"), "{err}");
        }
        let same = Overrides {
            gram_len: Some(3),
            budget_share: Some(0.5),
            fallback_min_bits: Some(64),
            zstd_level: Some(3),
            block_records: Some(4),
            object_records: Some(8),
            max_lines: Some(18),
            seed: Some(11),
        };
        let mut c = cfg("test_subject", same);
        c.corpus = Corpus::Lines(fixture("no-such-corpus.txt"));
        let err = run(&c).expect_err("missing corpus");
        assert!(err.contains("no-such-corpus"), "{err}");
    }

    #[test]
    fn run_measures_the_fixture_end_to_end() {
        let r = run(&cfg("test_subject", Overrides::default())).expect("run");
        assert!(r.objects >= 2);
        let report = &r.report;
        assert!(report.checked.problems.is_empty(), "{:?}", report.checked);
        assert!(report.checked.misses.is_empty(), "{:?}", report.checked);
        let stamp = report.stamp.as_ref().expect("stamp");
        assert_eq!(stamp.corpus_file, "corpus.txt");
        assert_eq!(stamp.corpus_sha256.len(), 64);
        let lines = report.lines();
        assert!(lines[0].starts_with("string_fingerprint stamp git="));
        assert!(lines.iter().all(|l| !l.contains("/tmp")));
        let by: BTreeMap<&str, f64> = report
            .figures
            .iter()
            .map(|f| (f.key.as_str(), f.value))
            .collect();
        assert_eq!(by["test_subject/lines"], 18.0);
        assert_eq!(by["test_subject/saturation/long_values"], 1.0);
        assert!(by["test_subject/contains/like/cells_tested_per_row"] > 0.0);
        assert!(by["test_subject/eval_equals_bloom_testable"] > 0.0);
        assert!(by.contains_key("test_subject/bloom/equals/false_positive_share"));
    }

    #[cfg(feature = "parquet-baseline")]
    #[test]
    fn reads_binary_and_utf8_parquet_columns_with_nulls() {
        use std::sync::Arc;

        use arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("hits.parquet");
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&b"http://a/x"[..]),
            None,
            Some(&b""[..]),
            Some(&b"\xffnot utf8"[..]),
        ]));
        let utf8: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some("title one"),
            Some("t2"),
            None,
        ]));
        let int: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let batch = RecordBatch::try_from_iter([("URL", binary), ("Title", utf8), ("Id", int)])
            .expect("batch");
        let file = File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let read = |column: &str, cap: u64| {
            read_values(
                &Corpus::ParquetColumn {
                    path: path.clone(),
                    column: column.to_string(),
                },
                cap,
            )
        };
        assert_eq!(
            read("URL", u64::MAX).expect("URL"),
            vec![
                b"http://a/x".to_vec(),
                Vec::new(),
                Vec::new(),
                b"\xffnot utf8".to_vec()
            ]
        );
        assert_eq!(
            read("Title", u64::MAX).expect("Title"),
            vec![
                Vec::new(),
                b"title one".to_vec(),
                b"t2".to_vec(),
                Vec::new()
            ]
        );
        assert_eq!(read("URL", 2).expect("cap").len(), 2);
        let err = read("Id", u64::MAX).expect_err("Int64 column");
        assert!(err.contains("neither Utf8 nor Binary"), "{err}");
        let err = read("Missing", u64::MAX).expect_err("no column");
        assert!(err.contains("no column \"Missing\""), "{err}");
    }

    #[cfg(feature = "parquet-baseline")]
    #[test]
    fn refuses_a_parquet_column_other_than_the_registered_one() {
        let parquet = |column: &str| {
            let mut c = cfg("test_subject", Overrides::default());
            c.corpus = Corpus::ParquetColumn {
                path: fixture("no-such-corpus.parquet"),
                column: column.to_string(),
            };
            run(&c).expect_err("no corpus")
        };
        let err = parquet("URL");
        assert!(err.contains("refusing --corpus-column \"URL\""), "{err}");
        assert!(!err.contains("no-such-corpus"), "{err}");
        // The registered column passes the check and fails on the missing file.
        let err = parquet("value");
        assert!(err.contains("no-such-corpus"), "{err}");
    }

    #[test]
    fn describe_counts_short_and_long_values() {
        let line = describe(&Corpus::Lines(fixture("corpus.txt")), u64::MAX).expect("describe");
        assert!(line.contains("values=18 "), "{line}");
        assert!(line.contains("non_utf8=0 "), "{line}");
        assert!(line.contains("over_1024_bytes=1"), "{line}");
    }
}
