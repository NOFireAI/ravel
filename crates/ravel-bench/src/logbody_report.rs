//! A report-only measurement of log bodies over a line corpus (issue #2977).
//!
//! Every line of the corpus becomes one log record whose body is the line,
//! written through the production [`RlogWriter`] and read back through the
//! production [`RlogReader`]. The report then measures, per block:
//!
//! - substring needles: which blocks hold an exact byte-substring match, which
//!   the production word-token Bloom keeps for a single-token needle, and which
//!   a per-block trigram Bloom ([`trigram`]) would keep at two false-positive
//!   targets, with that filter's size;
//! - body pages: the production page size and its decode throughput, against a
//!   template split ([`template`]) encoded with the production integer and
//!   string encoders, checked to rebuild every body byte for byte;
//! - integer pages: the production page size and decode throughput against a
//!   FastLanes-style transposed bit-packed layout ([`fastlanes`]).
//!
//! Nothing here changes what any crate writes or reads; the Bloom, template,
//! and FastLanes code exists only in this module.

pub mod fastlanes;
pub mod template;
pub mod trigram;

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ravel_logseg::encoding::{Enc, decode_i64, decode_strings};
use ravel_logseg::page::{DEFAULT_MAX_UNCOMP, SealedPage, read_page, seal_page, smallest_stored};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::record::{
    COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_STREAM_REF, COL_TS,
};
use ravel_logseg::rlog_codec::{decode_gcd_i64, i64_candidates, string_candidates};
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::{
    AttrValue, ColumnSelection, FieldSel, LogRecord, ObjectIdentity, Predicate, RlogConfig,
    RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_types::logstream::log_stream_id;
use serde::Serialize;

use self::fastlanes::Variant;
use self::trigram::{TrigramBloom, distinct_trigrams, mix64};

/// Runs per timed decode; the report states the median.
pub const DEFAULT_DECODE_RUNS: usize = 5;
/// The trigram Bloom false-positive targets the report sizes.
pub const FPR_TARGETS: [f64; 2] = [0.01, 0.001];
/// The note a needle gets when the word-token Bloom cannot answer it.
pub const NOT_A_SINGLE_TOKEN: &str = "not applicable: not a single token";

/// How a corpus line becomes a body, as the report states it.
pub const LINE_RULE: &str = "A line is the bytes up to a 0x0A byte; the 0x0A and one 0x0D \
directly before it are dropped, and a final line without 0x0A is kept. Invalid UTF-8 is replaced \
with U+FFFD (counted in lines_lossy_utf8); every figure is computed on the stored body.";
/// How a needles file is read, as the report states it.
pub const NEEDLE_RULE: &str = "One needle per line, bytes kept verbatim except the line ending; \
empty lines are skipped.";
/// The synthetic record fields, as the report states them.
pub const RECORD_RULE: &str = "One stream (fixed resource service.name=logbody-report). ts_ns of \
line i (0-based) is 1_700_000_000_000_000_000 + i * 1_000_000 + splitmix64(i) mod 1_000_000, \
strictly increasing; observed_ts_ns equals ts_ns; severity_num 9, severity_text INFO, flags 0, \
no attributes.";
/// What the timed decodes include.
pub const CLOCK: &str = "std::time::Instant (monotonic); each figure is decoded bytes divided by \
the median of the runs, in MB/s (10^6 bytes per second). body_encoding.decode_mb_per_s times the \
full page read: envelope parse, zstd decompression when the page is compressed, and the string \
codec. integer_pages' current_decode_mb_per_s and fastlanes_*_decode_mb_per_s time only the \
integer codec decode (decode_i64 or decode_gcd_i64 on the production side, fastlanes::decode on \
the FastLanes side) on bytes already unsealed before the timer starts: both exclude the envelope \
parse, the crc check, and zstd decompression when the production page is compressed, so the two \
are compared on the codec alone. The enveloped byte-size figures (current_bytes, \
fastlanes_*_enveloped_bytes) are unaffected and still include that envelope. Integer throughput \
divides by values * 8 bytes on both sides, so a FastLanes page whose last vector is padded to \
1024 values pays for the padding in fastlanes_*_decode_mb_per_s; \
fastlanes_*_padded_decode_mb_per_s divides the same time by padded_values * 8, the values \
fastlanes::decode unpacks, and the two agree when values is a multiple of 1024.";

const BASE_TS_NS: i64 = 1_700_000_000_000_000_000;
const TS_STEP_NS: i64 = 1_000_000;

/// What to measure and how.
#[derive(Clone, Debug)]
pub struct Config {
    pub corpus: PathBuf,
    pub needles: PathBuf,
    /// Stop after this many lines; `None` reads the whole corpus.
    pub max_lines: Option<u64>,
    /// The writer's `block_target_records`.
    pub block_records: usize,
    /// Records per RLOG object; the corpus is cut into objects of this many.
    pub object_records: usize,
    pub decode_runs: usize,
    /// The run date, stamped verbatim.
    pub date: String,
    /// The git sha, stamped verbatim.
    pub git_sha: String,
    /// The host CPU model, stamped verbatim; throughput figures depend on it.
    pub cpu_model: String,
}

impl Config {
    /// The production writer defaults for blocks and row groups, one object
    /// per row group's worth of default blocks.
    pub fn new(corpus: PathBuf, needles: PathBuf, date: String, git_sha: String) -> Self {
        let d = RlogConfig::default();
        Config {
            corpus,
            needles,
            max_lines: None,
            block_records: d.block_target_records,
            object_records: d.block_target_records * d.group_target_blocks,
            decode_runs: DEFAULT_DECODE_RUNS,
            date,
            git_sha,
            cpu_model: "unknown".to_string(),
        }
    }

    fn rlog(&self) -> RlogConfig {
        RlogConfig {
            block_target_records: self.block_records,
            ..RlogConfig::default()
        }
    }
}

#[derive(Debug)]
pub struct ReportError(String);

impl fmt::Display for ReportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReportError {}

fn fail(msg: impl Into<String>) -> ReportError {
    ReportError(msg.into())
}

// ---------------------------------------------------------------------------
// Report shape
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct Report {
    pub stamp: Stamp,
    pub substring: SubstringSection,
    pub trigram_sizing: Vec<TrigramSizing>,
    pub body_encoding: BodySection,
    pub integer_pages: IntegerSection,
}

#[derive(Debug, Serialize)]
pub struct Stamp {
    pub corpus_path: String,
    pub line_count: u64,
    pub byte_count: u64,
    pub sha256: String,
    /// Bytes of every stored body, as [`LINE_RULE`] makes it.
    pub body_bytes: u64,
    pub git_sha: String,
    pub cpu_model: String,
    pub date: String,
    pub block_target_records: usize,
    pub block_max_bytes: usize,
    pub group_target_blocks: usize,
    pub zstd_level: i32,
    pub object_records: usize,
    pub objects: u64,
    pub blocks: u64,
    pub compressed_object_bytes: u64,
    pub lines_lossy_utf8: u64,
    pub decode_runs: usize,
    pub clock: &'static str,
    pub line_rule: &'static str,
    pub needle_rule: &'static str,
    pub record_rule: &'static str,
}

#[derive(Debug, Serialize)]
pub struct SubstringSection {
    pub word_bloom_rule: &'static str,
    pub needles: Vec<NeedleReport>,
}

#[derive(Debug, Serialize)]
pub struct NeedleReport {
    pub needle: String,
    pub total_blocks: u64,
    pub exact_match_blocks: u64,
    pub exact_match_lines: u64,
    pub word_bloom: WordBloom,
    pub trigram_set_blocks: u64,
    pub trigram_bloom: Vec<TrigramKeep>,
}

/// The word-token Bloom's answer for one needle: the blocks it keeps, or the
/// reason it cannot answer.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum WordBloom {
    Kept {
        kept_blocks: u64,
        word_match_blocks: u64,
    },
    NotApplicable(&'static str),
}

#[derive(Debug, Serialize)]
pub struct TrigramKeep {
    pub target_fpr: f64,
    pub kept_blocks: u64,
    /// Kept blocks without an exact match.
    pub false_positive_blocks: u64,
    /// Of those, the blocks the filter kept although some needle trigram is
    /// absent from the block: the Bloom's own error, not the trigram set's.
    pub bloom_false_positive_blocks: u64,
}

#[derive(Debug, Serialize)]
pub struct TrigramSizing {
    pub target_fpr: f64,
    pub keys: &'static str,
    pub blocks: u64,
    pub distinct_trigrams_min: u64,
    pub distinct_trigrams_max: u64,
    pub distinct_trigrams_total: u64,
    pub hashes_min: u32,
    pub hashes_max: u32,
    pub bytes_per_block_min: u64,
    pub bytes_per_block_max: u64,
    pub bytes_per_block_mean: f64,
    pub filter_bytes_total: u64,
    pub share_of_compressed_object_bytes: f64,
    pub needle_kept_blocks: u64,
    pub needle_exact_blocks: u64,
    pub needle_false_positive_blocks: u64,
}

#[derive(Debug, Serialize)]
pub struct BodySection {
    pub template_rule: &'static str,
    pub layout: &'static str,
    pub pages: u64,
    pub lines_checked: u64,
    pub mismatch_count: u64,
    pub template_count_corpus: u64,
    pub variable_count: u64,
    pub page_dir_body_bytes: u64,
    pub page_dir_body_dict_bytes: u64,
    pub page_dir_encodings: BTreeMap<String, u64>,
    pub block_local_bytes: u64,
    pub block_local_encodings: BTreeMap<String, u64>,
    pub decoded_bytes: u64,
    pub decode_mb_per_s: f64,
    pub template_ids_bytes: u64,
    pub template_dict_bytes: u64,
    pub variables_bytes: u64,
    pub template_total_bytes: u64,
    pub template_total_over_block_local: f64,
    pub template_total_over_page_dir: f64,
    pub per_page: Vec<BodyPage>,
}

#[derive(Debug, Serialize)]
pub struct BodyPage {
    pub block: u64,
    pub records: u64,
    pub page_dir_enc: String,
    pub page_dir_bytes: u64,
    pub block_local_enc: String,
    pub block_local_bytes: u64,
    pub decoded_bytes: u64,
    pub decode_mb_per_s: f64,
    pub templates: u64,
    pub template_ids_enc: String,
    pub template_ids_bytes: u64,
    pub template_dict_enc: String,
    pub template_dict_bytes: u64,
    pub variables: u64,
    pub variables_enc: String,
    pub variables_bytes: u64,
    pub template_total_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct IntegerSection {
    pub layout: &'static str,
    pub columns: Vec<IntegerColumn>,
    pub excluded: Vec<ExcludedColumn>,
    pub per_page: Vec<IntegerPage>,
}

#[derive(Debug, Serialize)]
pub struct IntegerColumn {
    pub column: &'static str,
    pub column_id: u32,
    pub pages: u64,
    pub values: u64,
    pub padded_values: u64,
    pub current_bytes: u64,
    pub current_encodings: BTreeMap<String, u64>,
    pub current_decode_mb_per_s: f64,
    pub fastlanes_for_bytes: u64,
    pub fastlanes_for_enveloped_bytes: u64,
    pub fastlanes_for_decode_mb_per_s: f64,
    pub fastlanes_for_padded_decode_mb_per_s: f64,
    pub fastlanes_delta_bytes: u64,
    pub fastlanes_delta_enveloped_bytes: u64,
    pub fastlanes_delta_decode_mb_per_s: f64,
    pub fastlanes_delta_padded_decode_mb_per_s: f64,
}

#[derive(Debug, Serialize)]
pub struct ExcludedColumn {
    pub column: &'static str,
    pub column_id: u32,
    pub pages: u64,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct IntegerPage {
    pub block: u64,
    pub column: &'static str,
    pub values: u64,
    pub padded_values: u64,
    pub current_enc: String,
    pub current_bytes: u64,
    pub current_decode_mb_per_s: f64,
    pub fastlanes_for_bytes: u64,
    pub fastlanes_for_enveloped_bytes: u64,
    pub fastlanes_for_decode_mb_per_s: f64,
    pub fastlanes_for_padded_decode_mb_per_s: f64,
    pub fastlanes_delta_bytes: u64,
    pub fastlanes_delta_enveloped_bytes: u64,
    pub fastlanes_delta_decode_mb_per_s: f64,
    pub fastlanes_delta_padded_decode_mb_per_s: f64,
}

impl Report {
    /// Every reason this report must not count as a result: an empty section,
    /// a template mismatch. Empty when it is one.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.stamp.line_count == 0 {
            out.push("stamp: the corpus has no lines".to_string());
        }
        if self.substring.needles.is_empty() {
            out.push("substring: no needles".to_string());
        }
        if self.trigram_sizing.is_empty() || self.trigram_sizing.iter().any(|s| s.blocks == 0) {
            out.push("trigram_sizing: no blocks sized".to_string());
        }
        if self.body_encoding.per_page.is_empty() {
            out.push("body_encoding: no body pages".to_string());
        }
        if self.body_encoding.mismatch_count != 0 {
            out.push(format!(
                "body_encoding: {} of {} lines did not rebuild byte for byte",
                self.body_encoding.mismatch_count, self.body_encoding.lines_checked
            ));
        }
        if self.integer_pages.columns.is_empty() || self.integer_pages.per_page.is_empty() {
            out.push("integer_pages: no integer pages".to_string());
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------

fn open_text(path: &Path) -> Result<Box<dyn BufRead>, ReportError> {
    let file = File::open(path).map_err(|e| fail(format!("open {}: {e}", path.display())))?;
    if path.extension().is_some_and(|x| x == "gz") {
        Ok(Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(
            file,
        ))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

/// The next line under [`LINE_RULE`], without its ending. `None` at the end.
fn next_line(input: &mut dyn BufRead, buf: &mut Vec<u8>) -> Result<Option<()>, ReportError> {
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

/// sha256 and byte count of the corpus file as stored (compressed, for .gz).
fn file_digest(path: &Path) -> Result<(String, u64), ReportError> {
    let mut file = File::open(path).map_err(|e| fail(format!("open {}: {e}", path.display())))?;
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buf = vec![0u8; 1 << 16];
    let mut total = 0u64;
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| fail(format!("read {}: {e}", path.display())))?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
        total += n as u64;
    }
    let hex = ctx
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok((hex, total))
}

/// Needles under [`NEEDLE_RULE`].
pub fn read_needles(path: &Path) -> Result<Vec<Vec<u8>>, ReportError> {
    let mut input = open_text(path)?;
    let mut buf = Vec::new();
    let mut out = Vec::new();
    while next_line(input.as_mut(), &mut buf)?.is_some() {
        if !buf.is_empty() {
            out.push(buf.clone());
        }
    }
    Ok(out)
}

/// `ts_ns` of corpus line `i` under [`RECORD_RULE`].
pub fn ts_of(i: u64) -> i64 {
    BASE_TS_NS + (i as i64) * TS_STEP_NS + (mix64(i) % TS_STEP_NS as u64) as i64
}

/// Whether `hay` holds `needle` as a byte substring. An empty needle matches.
pub fn contains(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

/// The single word token a needle is, if it is one: every byte alphanumeric
/// ASCII and the tokenizer returning exactly one token for it.
fn single_token(needle: &[u8]) -> Option<Vec<u8>> {
    if needle.is_empty() || !needle.iter().all(u8::is_ascii_alphanumeric) {
        return None;
    }
    let text = std::str::from_utf8(needle).ok()?;
    let mut t = tokens(text);
    (t.len() == 1).then(|| t.remove(0))
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------

struct NeedleAcc {
    needle: Vec<u8>,
    token: Option<Vec<u8>>,
    total_blocks: u64,
    exact_blocks: u64,
    exact_lines: u64,
    word_kept: u64,
    word_match_blocks: u64,
    trigram_set_blocks: u64,
    tri_kept: [u64; FPR_TARGETS.len()],
    tri_fp: [u64; FPR_TARGETS.len()],
    tri_bloom_fp: [u64; FPR_TARGETS.len()],
}

#[derive(Default)]
struct FprAcc {
    blocks: u64,
    keys_min: u64,
    keys_max: u64,
    keys_total: u64,
    k_min: u32,
    k_max: u32,
    bytes_min: u64,
    bytes_max: u64,
    bytes_total: u64,
}

impl FprAcc {
    fn add(&mut self, keys: u64, k: u32, bytes: u64) {
        if self.blocks == 0 {
            (self.keys_min, self.k_min, self.bytes_min) = (keys, k, bytes);
        }
        self.blocks += 1;
        self.keys_min = self.keys_min.min(keys);
        self.keys_max = self.keys_max.max(keys);
        self.keys_total += keys;
        self.k_min = self.k_min.min(k);
        self.k_max = self.k_max.max(k);
        self.bytes_min = self.bytes_min.min(bytes);
        self.bytes_max = self.bytes_max.max(bytes);
        self.bytes_total += bytes;
    }
}

#[derive(Default)]
struct IntAcc {
    pages: u64,
    values: u64,
    padded_values: u64,
    current_bytes: u64,
    encodings: BTreeMap<String, u64>,
    current_secs: f64,
    for_bytes: u64,
    for_enveloped: u64,
    for_secs: f64,
    delta_bytes: u64,
    delta_enveloped: u64,
    delta_secs: f64,
    excluded_pages: u64,
    excluded_reason: Option<String>,
}

const INT_COLUMNS: [(&str, u32); 5] = [
    ("ts", COL_TS),
    ("observed_ts", COL_OBSERVED_TS),
    ("stream_ref", COL_STREAM_REF),
    ("severity_num", COL_SEVERITY_NUM),
    ("flags", COL_FLAGS),
];

struct State {
    cfg: Config,
    rlog: RlogConfig,
    needles: Vec<NeedleAcc>,
    fpr: Vec<FprAcc>,
    blocks: u64,
    objects: u64,
    object_bytes: u64,
    // Body encoding.
    templates: HashSet<Vec<u8>>,
    body_pages: Vec<BodyPage>,
    body_secs: f64,
    page_dir_body_dict_bytes: u64,
    page_dir_encodings: BTreeMap<String, u64>,
    block_local_encodings: BTreeMap<String, u64>,
    lines_checked: u64,
    mismatches: u64,
    variable_count: u64,
    // Integer pages, indexed like INT_COLUMNS.
    ints: Vec<IntAcc>,
    int_pages: Vec<IntegerPage>,
}

fn enc_name(enc: Enc) -> String {
    format!("{enc:?}")
}

fn bump(map: &mut BTreeMap<String, u64>, key: String) {
    *map.entry(key).or_default() += 1;
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort_unstable();
    v[v.len() / 2]
}

/// Median wall time of `runs` calls of `f`, at least one nanosecond.
fn time_median<T>(runs: usize, mut f: impl FnMut() -> T) -> f64 {
    let samples = (0..runs.max(1))
        .map(|_| {
            let t = Instant::now();
            black_box(f());
            t.elapsed()
        })
        .collect();
    median(samples).as_secs_f64().max(1e-9)
}

fn mb_per_s(bytes: u64, secs: f64) -> f64 {
    bytes as f64 / secs / 1e6
}

/// Values `fastlanes::decode` actually unpacks for a page of `n` values.
fn padded_len(n: usize) -> u64 {
    (n.div_ceil(fastlanes::VECTOR) * fastlanes::VECTOR) as u64
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}

fn seal(candidates: Vec<(Enc, Vec<u8>)>, level: i32) -> SealedPage {
    smallest_stored(candidates, level).unwrap_or_else(|| seal_page(Enc::Plain, Vec::new(), level))
}

/// The payload of a sealed page after its envelope, as the reader gets it.
fn unseal(page: &SealedPage) -> Result<Vec<u8>, ReportError> {
    let mut buf = Vec::with_capacity(page.stored.len());
    let desc = page.append(&mut buf, 0);
    read_page(&buf, &desc, DEFAULT_MAX_UNCOMP).map_err(|e| fail(format!("read page: {e}")))
}

/// Decodes the integer codec alone, on bytes already unsealed: no envelope
/// parse, crc check, or decompression.
fn decode_int_from_raw(enc: Enc, raw: &[u8], count: usize) -> Result<Vec<i64>, ReportError> {
    match enc {
        Enc::GcdI64 => decode_gcd_i64(raw, count).map_err(|e| fail(e.to_string())),
        enc => decode_i64(enc, raw, count).map_err(|e| fail(e.to_string())),
    }
}

fn decode_int_page(page: &SealedPage, count: usize) -> Result<Vec<i64>, ReportError> {
    let raw = unseal(page)?;
    decode_int_from_raw(page.enc, &raw, count)
}

fn decode_str_page(page: &SealedPage, count: usize) -> Result<Vec<Vec<u8>>, ReportError> {
    let raw = unseal(page)?;
    decode_strings(page.enc, &raw, count).map_err(|e| fail(e.to_string()))
}

/// Counts rows whose body does not rebuild from the decoded template-split
/// pages. `splits` is the pre-encode ground truth: each row's variable-window
/// length comes from `splits[row].vars.len()`, not from the decoded
/// template, so the cursor into `got_vars` advances by the same amount for
/// every row regardless of whether that row's own id or dictionary lookup
/// succeeds. A row whose own lookup is corrupted still only costs that one
/// row, instead of shifting every later row's window.
fn rebuild_mismatches(
    splits: &[template::Split<'_>],
    got_ids: &[i64],
    got_dict: &[Vec<u8>],
    got_vars: &[Vec<u8>],
    bodies: &[&[u8]],
) -> u64 {
    let mut at = 0usize;
    let mut mismatches = 0u64;
    for (row, body) in bodies.iter().enumerate() {
        let n = splits[row].vars.len();
        let v = got_vars.get(at..at + n);
        at += n;
        let rebuilt = got_ids
            .get(row)
            .and_then(|&id| got_dict.get(usize::try_from(id).ok()?))
            .zip(v)
            .and_then(|(t, v)| template::reconstruct(t, v));
        mismatches += u64::from(rebuilt.as_deref() != Some(*body));
    }
    mismatches
}

/// The stored length and encoding PAGE_DIR gives `column` in `block`, if the
/// block has a value page for it (a presence bitmap page is skipped).
fn page_dir_page(dir: &PageDir, block: u32, column: u32) -> Option<(Enc, u64)> {
    dir.block_pages(block)?
        .into_iter()
        .filter(|p| p.desc.column_id == column && p.desc.enc != Enc::Bitmap)
        .map(|p| (p.desc.enc, p.desc.len))
        .next_back()
}

/// Stored bytes of every row-group dictionary page `column` has.
fn page_dir_dict_bytes(dir: &PageDir, column: u32) -> u64 {
    dir.groups
        .iter()
        .flat_map(|g| g.chunks.iter())
        .filter(|c| c.column_id == column)
        .filter_map(|c| c.dict_page())
        .map(|p| p.len)
        .sum()
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// Reads the corpus and needles named in `cfg` and builds the report.
/// Inconsistencies between what was written and what reads back are errors;
/// the checks that make a finished report unusable are [`Report::problems`].
pub fn run(cfg: &Config) -> Result<Report, ReportError> {
    if cfg.block_records == 0 || cfg.object_records == 0 {
        return Err(fail("block and object record counts must be nonzero"));
    }
    if cfg.decode_runs == 0 {
        return Err(fail("decode_runs must be nonzero"));
    }
    let needles = read_needles(&cfg.needles)?;
    let (sha256, byte_count) = file_digest(&cfg.corpus)?;
    let rlog = cfg.rlog();
    let mut st = State {
        cfg: cfg.clone(),
        rlog,
        needles: needles
            .into_iter()
            .map(|needle| NeedleAcc {
                token: single_token(&needle),
                needle,
                total_blocks: 0,
                exact_blocks: 0,
                exact_lines: 0,
                word_kept: 0,
                word_match_blocks: 0,
                trigram_set_blocks: 0,
                tri_kept: [0; FPR_TARGETS.len()],
                tri_fp: [0; FPR_TARGETS.len()],
                tri_bloom_fp: [0; FPR_TARGETS.len()],
            })
            .collect(),
        fpr: FPR_TARGETS.iter().map(|_| FprAcc::default()).collect(),
        blocks: 0,
        objects: 0,
        object_bytes: 0,
        templates: HashSet::new(),
        body_pages: Vec::new(),
        body_secs: 0.0,
        page_dir_body_dict_bytes: 0,
        page_dir_encodings: BTreeMap::new(),
        block_local_encodings: BTreeMap::new(),
        lines_checked: 0,
        mismatches: 0,
        variable_count: 0,
        ints: INT_COLUMNS.iter().map(|_| IntAcc::default()).collect(),
        int_pages: Vec::new(),
    };

    let mut input = open_text(&cfg.corpus)?;
    let mut buf = Vec::new();
    let mut line_count = 0u64;
    let mut lossy = 0u64;
    let mut body_bytes = 0u64;
    let mut object: Vec<String> = Vec::new();
    while cfg.max_lines.is_none_or(|m| line_count < m)
        && next_line(input.as_mut(), &mut buf)?.is_some()
    {
        let body = match String::from_utf8(buf.clone()) {
            Ok(s) => s,
            Err(_) => {
                lossy += 1;
                String::from_utf8_lossy(&buf).into_owned()
            }
        };
        body_bytes += body.len() as u64;
        object.push(body);
        line_count += 1;
        if object.len() == cfg.object_records {
            st.object(line_count - object.len() as u64, &object)?;
            object.clear();
        }
    }
    if !object.is_empty() {
        st.object(line_count - object.len() as u64, &object)?;
    }

    Ok(st.finish(Stamp {
        corpus_path: cfg.corpus.display().to_string(),
        line_count,
        byte_count,
        sha256,
        body_bytes,
        git_sha: cfg.git_sha.clone(),
        cpu_model: cfg.cpu_model.clone(),
        date: cfg.date.clone(),
        block_target_records: rlog.block_target_records,
        block_max_bytes: rlog.block_max_bytes,
        group_target_blocks: rlog.group_target_blocks,
        zstd_level: rlog.zstd_level,
        object_records: cfg.object_records,
        objects: 0,
        blocks: 0,
        compressed_object_bytes: 0,
        lines_lossy_utf8: lossy,
        decode_runs: cfg.decode_runs,
        clock: CLOCK,
        line_rule: LINE_RULE,
        needle_rule: NEEDLE_RULE,
        record_rule: RECORD_RULE,
    }))
}

/// One block read back from an object.
struct Block {
    bodies: Vec<Vec<u8>>,
    /// Values per [`INT_COLUMNS`] entry.
    ints: Vec<Vec<i64>>,
}

impl State {
    /// Writes `bodies` (corpus lines from `first_line` on) as one RLOG object
    /// and measures every block of it.
    fn object(&mut self, first_line: u64, bodies: &[String]) -> Result<(), ReportError> {
        let identity = ObjectIdentity {
            tenant_hash: [0x5a; 16],
            shard: 0,
            writer_id: [0x2e; 16],
            writer_epoch: 1,
            writer_seq: self.objects,
        };
        let res = vec![(
            "service.name".to_string(),
            AttrValue::Str("logbody-report".to_string()),
        )];
        let stream_id = log_stream_id(&res, "logbody_report", "1", &[]);
        let stream_attrs = stream_attrs_bytes(&res, "logbody_report", "1", &[]);
        let mut writer = RlogWriter::new(self.rlog, identity);
        for (i, body) in bodies.iter().enumerate() {
            let ts = ts_of(first_line + i as u64);
            writer
                .push(LogRecord {
                    stream_id,
                    stream_attrs: stream_attrs.clone(),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".to_string(),
                    body: body.clone(),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .map_err(|e| fail(format!("push: {e}")))?;
        }
        let bytes = writer.finish().map_err(|e| fail(format!("finish: {e}")))?;
        self.object_bytes += bytes.len() as u64;
        self.objects += 1;

        let reader =
            RlogReader::new(&bytes, &self.rlog).map_err(|e| fail(format!("open object: {e}")))?;
        let dirs = reader.directories();
        let dir = dirs.page_dir().clone();
        self.page_dir_body_dict_bytes += page_dir_dict_bytes(&dir, COL_BODY);

        // Word-token Bloom: the production scan's own bloom pruning count.
        for n in &mut self.needles {
            let Some(token) = &n.token else { continue };
            let word = String::from_utf8_lossy(token).into_owned();
            let scan = reader
                .scan_blocks(
                    &Predicate::HasWord {
                        field: FieldSel::Body,
                        word,
                    },
                    &[],
                    &ColumnSelection::fixed_only(),
                )
                .map_err(|e| fail(format!("word scan: {e}")))?;
            let stats = scan.stats();
            if stats.bloom_degraded {
                return Err(fail("BLOOM section degraded on a freshly written object"));
            }
            n.word_kept += u64::from(stats.blocks_after_bloom);
        }

        let columns = ColumnSelection::fixed_only()
            .with_body()
            .with_observed_ts()
            .with_severity_num()
            .with_flags();
        let mut scan = reader
            .scan_blocks(
                &Predicate::TsRange {
                    min_ns: i64::MIN,
                    max_ns: i64::MAX,
                },
                &[],
                &columns,
            )
            .map_err(|e| fail(format!("scan: {e}")))?;
        let mut next = 0usize;
        while let Some(index) = scan.next_block_index() {
            let Some(view) = scan
                .next_block_columnar(bytes.as_slice())
                .map_err(|e| fail(format!("decode block {index}: {e}")))?
            else {
                break;
            };
            let n = view.surviving_count();
            let mut block = Block {
                bodies: Vec::with_capacity(n),
                ints: vec![Vec::with_capacity(n); INT_COLUMNS.len()],
            };
            for i in 0..n {
                let missing = || fail(format!("block {index} row {i}: column not decoded"));
                block
                    .bodies
                    .push(view.body(i).ok_or_else(missing)?.to_vec());
                let row = [
                    view.ts(i),
                    view.observed_ts(i),
                    view.stream_ref(i).map(i64::from),
                    view.severity_num(i),
                    view.flags(i),
                ];
                for (col, v) in block.ints.iter_mut().zip(row) {
                    col.push(v.ok_or_else(missing)?);
                }
            }
            for (k, body) in block.bodies.iter().enumerate() {
                if bodies.get(next + k).map(String::as_bytes) != Some(body.as_slice()) {
                    return Err(fail(format!(
                        "block {index} row {k}: body read back differs from corpus line {}",
                        first_line + (next + k) as u64
                    )));
                }
            }
            next += block.bodies.len();
            self.block(&dir, index as u32, &block)?;
            self.blocks += 1;
        }
        if next != bodies.len() {
            return Err(fail(format!(
                "object read back {next} records of {}",
                bodies.len()
            )));
        }
        Ok(())
    }

    fn block(&mut self, dir: &PageDir, index: u32, block: &Block) -> Result<(), ReportError> {
        let global = self.blocks;
        let level = self.rlog.zstd_level;
        let runs = self.cfg.decode_runs;

        // Substring needles and the trigram filters.
        let keys = distinct_trigrams(&block.bodies);
        let blooms: Vec<TrigramBloom> = FPR_TARGETS
            .iter()
            .map(|&p| TrigramBloom::build(&keys, p))
            .collect();
        for (acc, bloom) in self.fpr.iter_mut().zip(&blooms) {
            acc.add(
                keys.len() as u64,
                bloom.hash_count(),
                bloom.byte_len() as u64,
            );
        }
        let token_sets: Option<HashSet<Vec<u8>>> =
            self.needles.iter().any(|n| n.token.is_some()).then(|| {
                block
                    .bodies
                    .iter()
                    .flat_map(|b| tokens(&String::from_utf8_lossy(b)))
                    .collect()
            });
        for n in &mut self.needles {
            n.total_blocks += 1;
            let lines = block
                .bodies
                .iter()
                .filter(|b| contains(b, &n.needle))
                .count() as u64;
            n.exact_lines += lines;
            let exact = lines > 0;
            n.exact_blocks += u64::from(exact);
            let set = trigram::trigrams(&n.needle).all(|t| keys.binary_search(&t).is_ok());
            n.trigram_set_blocks += u64::from(set);
            for (f, bloom) in blooms.iter().enumerate() {
                let kept = bloom.may_match(&n.needle);
                if exact && !kept {
                    return Err(fail(format!(
                        "trigram filter dropped block {global} holding needle {:?}",
                        String::from_utf8_lossy(&n.needle)
                    )));
                }
                n.tri_kept[f] += u64::from(kept);
                n.tri_fp[f] += u64::from(kept && !exact);
                n.tri_bloom_fp[f] += u64::from(kept && !set);
            }
            if let (Some(token), Some(sets)) = (&n.token, &token_sets) {
                n.word_match_blocks += u64::from(sets.contains(token));
            }
        }

        self.body(dir, index, block, global, level, runs)?;
        self.integers(dir, index, block, global, level, runs)
    }

    fn body(
        &mut self,
        dir: &PageDir,
        index: u32,
        block: &Block,
        global: u64,
        level: i32,
        runs: usize,
    ) -> Result<(), ReportError> {
        let values: Vec<&[u8]> = block.bodies.iter().map(Vec::as_slice).collect();
        let (pd_enc, pd_bytes) = page_dir_page(dir, index, COL_BODY)
            .ok_or_else(|| fail(format!("block {global}: PAGE_DIR lists no body page")))?;
        bump(&mut self.page_dir_encodings, enc_name(pd_enc));
        let local = seal(string_candidates(&values), level);
        bump(&mut self.block_local_encodings, enc_name(local.enc));
        if decode_str_page(&local, values.len())? != block.bodies {
            return Err(fail(format!(
                "block {global}: block-local body page does not decode"
            )));
        }
        let decoded_bytes: u64 = values.iter().map(|v| v.len() as u64).sum();
        let secs = time_median(runs, || decode_str_page(&local, values.len()));
        self.body_secs += secs;

        // Template split: block-local template dictionary, per-row ids into
        // it, and every variable in row order as one string page.
        let splits: Vec<template::Split<'_>> = values.iter().map(|v| template::split(v)).collect();
        let templates: Vec<&[u8]> = splits.iter().map(|s| s.template.as_slice()).collect();
        let mut dict = templates.clone();
        dict.sort_unstable();
        dict.dedup();
        let ids: Vec<i64> = templates
            .iter()
            .map(|t| dict.partition_point(|d| d < t) as i64)
            .collect();
        let vars: Vec<&[u8]> = splits.iter().flat_map(|s| s.vars.iter().copied()).collect();
        for t in &dict {
            self.templates.insert(t.to_vec());
        }
        self.variable_count += vars.len() as u64;
        let ids_page = seal(i64_candidates(&ids), level);
        let dict_page = seal(string_candidates(&dict), level);
        let vars_page = seal(string_candidates(&vars), level);

        let got_ids = decode_int_page(&ids_page, ids.len())?;
        let got_dict = decode_str_page(&dict_page, dict.len())?;
        let got_vars = decode_str_page(&vars_page, vars.len())?;
        self.lines_checked += values.len() as u64;
        self.mismatches += rebuild_mismatches(&splits, &got_ids, &got_dict, &got_vars, &values);

        let total =
            (ids_page.stored.len() + dict_page.stored.len() + vars_page.stored.len()) as u64;
        self.body_pages.push(BodyPage {
            block: global,
            records: values.len() as u64,
            page_dir_enc: enc_name(pd_enc),
            page_dir_bytes: pd_bytes,
            block_local_enc: enc_name(local.enc),
            block_local_bytes: local.stored.len() as u64,
            decoded_bytes,
            decode_mb_per_s: mb_per_s(decoded_bytes, secs),
            templates: dict.len() as u64,
            template_ids_enc: enc_name(ids_page.enc),
            template_ids_bytes: ids_page.stored.len() as u64,
            template_dict_enc: enc_name(dict_page.enc),
            template_dict_bytes: dict_page.stored.len() as u64,
            variables: vars.len() as u64,
            variables_enc: enc_name(vars_page.enc),
            variables_bytes: vars_page.stored.len() as u64,
            template_total_bytes: total,
        });
        Ok(())
    }

    fn integers(
        &mut self,
        dir: &PageDir,
        index: u32,
        block: &Block,
        global: u64,
        level: i32,
        runs: usize,
    ) -> Result<(), ReportError> {
        for (c, &(name, column)) in INT_COLUMNS.iter().enumerate() {
            let Some((pd_enc, pd_bytes)) = page_dir_page(dir, index, column) else {
                return Err(fail(format!(
                    "block {global}: PAGE_DIR lists no {name} page"
                )));
            };
            let acc = &mut self.ints[c];
            if pd_enc == Enc::ColumnRef {
                acc.excluded_pages += 1;
                acc.excluded_reason = Some(format!(
                    "stored as {} (tag {}): a reference to another column, no integer payload",
                    enc_name(pd_enc),
                    pd_enc as u8
                ));
                continue;
            }
            let values = &block.ints[c];
            let current = seal(i64_candidates(values), level);
            if current.enc != pd_enc || current.stored.len() as u64 != pd_bytes {
                return Err(fail(format!(
                    "block {global} {name}: re-encoding gives {} {} bytes, PAGE_DIR has {} {pd_bytes}",
                    enc_name(current.enc),
                    current.stored.len(),
                    enc_name(pd_enc)
                )));
            }
            if decode_int_page(&current, values.len())? != *values {
                return Err(fail(format!("block {global} {name}: page does not decode")));
            }
            let decoded = values.len() as u64 * 8;
            let padded_values = padded_len(values.len());
            let current_raw = unseal(&current)?;
            let cur_secs = time_median(runs, || {
                decode_int_from_raw(current.enc, &current_raw, values.len())
            });
            let mut fl = [(0u64, 0u64, 0f64); 2];
            for (slot, variant) in fl.iter_mut().zip([Variant::For, Variant::Delta]) {
                let page = fastlanes::encode(values, variant);
                if fastlanes::decode(&page, variant).as_ref() != Some(values) {
                    return Err(fail(format!(
                        "block {global} {name}: {} does not round-trip",
                        variant.name()
                    )));
                }
                let secs = time_median(runs, || fastlanes::decode(&page, variant));
                let enveloped = seal_page(Enc::Plain, page.clone(), level).stored.len() as u64;
                *slot = (page.len() as u64, enveloped, secs);
            }
            acc.pages += 1;
            acc.values += values.len() as u64;
            acc.padded_values += padded_values;
            acc.current_bytes += pd_bytes;
            bump(&mut acc.encodings, enc_name(pd_enc));
            acc.current_secs += cur_secs;
            acc.for_bytes += fl[0].0;
            acc.for_enveloped += fl[0].1;
            acc.for_secs += fl[0].2;
            acc.delta_bytes += fl[1].0;
            acc.delta_enveloped += fl[1].1;
            acc.delta_secs += fl[1].2;
            self.int_pages.push(IntegerPage {
                block: global,
                column: name,
                values: values.len() as u64,
                padded_values,
                current_enc: enc_name(pd_enc),
                current_bytes: pd_bytes,
                current_decode_mb_per_s: mb_per_s(decoded, cur_secs),
                fastlanes_for_bytes: fl[0].0,
                fastlanes_for_enveloped_bytes: fl[0].1,
                fastlanes_for_decode_mb_per_s: mb_per_s(decoded, fl[0].2),
                fastlanes_for_padded_decode_mb_per_s: mb_per_s(padded_values * 8, fl[0].2),
                fastlanes_delta_bytes: fl[1].0,
                fastlanes_delta_enveloped_bytes: fl[1].1,
                fastlanes_delta_decode_mb_per_s: mb_per_s(decoded, fl[1].2),
                fastlanes_delta_padded_decode_mb_per_s: mb_per_s(padded_values * 8, fl[1].2),
            });
        }
        Ok(())
    }

    fn finish(self, mut stamp: Stamp) -> Report {
        stamp.objects = self.objects;
        stamp.blocks = self.blocks;
        stamp.compressed_object_bytes = self.object_bytes;

        let needles: Vec<NeedleReport> = self
            .needles
            .iter()
            .map(|n| NeedleReport {
                needle: String::from_utf8_lossy(&n.needle).into_owned(),
                total_blocks: n.total_blocks,
                exact_match_blocks: n.exact_blocks,
                exact_match_lines: n.exact_lines,
                word_bloom: match n.token {
                    Some(_) => WordBloom::Kept {
                        kept_blocks: n.word_kept,
                        word_match_blocks: n.word_match_blocks,
                    },
                    None => WordBloom::NotApplicable(NOT_A_SINGLE_TOKEN),
                },
                trigram_set_blocks: n.trigram_set_blocks,
                trigram_bloom: FPR_TARGETS
                    .iter()
                    .enumerate()
                    .map(|(f, &p)| TrigramKeep {
                        target_fpr: p,
                        kept_blocks: n.tri_kept[f],
                        false_positive_blocks: n.tri_fp[f],
                        bloom_false_positive_blocks: n.tri_bloom_fp[f],
                    })
                    .collect(),
            })
            .collect();

        let trigram_sizing = FPR_TARGETS
            .iter()
            .zip(&self.fpr)
            .enumerate()
            .map(|(f, (&p, acc))| {
                let kept: u64 = self.needles.iter().map(|n| n.tri_kept[f]).sum();
                let exact: u64 = self.needles.iter().map(|n| n.exact_blocks).sum();
                let false_positive: u64 = self.needles.iter().map(|n| n.tri_fp[f]).sum();
                TrigramSizing {
                    target_fpr: p,
                    keys: "distinct byte trigrams over the block's bodies; a needle under three \
                           bytes has no trigram and keeps every block",
                    blocks: acc.blocks,
                    distinct_trigrams_min: acc.keys_min,
                    distinct_trigrams_max: acc.keys_max,
                    distinct_trigrams_total: acc.keys_total,
                    hashes_min: acc.k_min,
                    hashes_max: acc.k_max,
                    bytes_per_block_min: acc.bytes_min,
                    bytes_per_block_max: acc.bytes_max,
                    bytes_per_block_mean: ratio(acc.bytes_total, acc.blocks),
                    filter_bytes_total: acc.bytes_total,
                    share_of_compressed_object_bytes: ratio(acc.bytes_total, self.object_bytes),
                    needle_kept_blocks: kept,
                    needle_exact_blocks: exact,
                    needle_false_positive_blocks: false_positive,
                }
            })
            .collect();

        let sum = |f: fn(&BodyPage) -> u64| self.body_pages.iter().map(f).sum::<u64>();
        let block_local_bytes = sum(|p| p.block_local_bytes);
        let template_total_bytes = sum(|p| p.template_total_bytes);
        let decoded_bytes = sum(|p| p.decoded_bytes);
        let page_dir_body_bytes = sum(|p| p.page_dir_bytes);
        let body_encoding = BodySection {
            template_rule: template::RULE,
            layout: "Per block: the distinct templates as one string page (sorted), each row's \
                     index into it as one i64 page, and every variable in row order as one string \
                     page; each page is the smallest stored candidate of the production encoders \
                     after the production zstd envelope. Compared with the block-local winner the \
                     writer computes for the body page (tag 1 or 7) in \
                     template_total_over_block_local, and with PAGE_DIR's body pages plus their \
                     row-group dictionary pages in template_total_over_page_dir. Decode \
                     throughput is measured on the block-local winner.",
            pages: self.body_pages.len() as u64,
            lines_checked: self.lines_checked,
            mismatch_count: self.mismatches,
            template_count_corpus: self.templates.len() as u64,
            variable_count: self.variable_count,
            page_dir_body_bytes,
            page_dir_body_dict_bytes: self.page_dir_body_dict_bytes,
            page_dir_encodings: self.page_dir_encodings,
            block_local_bytes,
            block_local_encodings: self.block_local_encodings,
            decoded_bytes,
            decode_mb_per_s: mb_per_s(decoded_bytes, self.body_secs.max(1e-9)),
            template_ids_bytes: sum(|p| p.template_ids_bytes),
            template_dict_bytes: sum(|p| p.template_dict_bytes),
            variables_bytes: sum(|p| p.variables_bytes),
            template_total_bytes,
            template_total_over_block_local: ratio(template_total_bytes, block_local_bytes),
            template_total_over_page_dir: ratio(
                template_total_bytes,
                page_dir_body_bytes + self.page_dir_body_dict_bytes,
            ),
            per_page: self.body_pages,
        };

        let mut columns = Vec::new();
        let mut excluded = Vec::new();
        for (&(name, column_id), acc) in INT_COLUMNS.iter().zip(self.ints) {
            if let Some(reason) = acc.excluded_reason {
                excluded.push(ExcludedColumn {
                    column: name,
                    column_id,
                    pages: acc.excluded_pages,
                    reason,
                });
            }
            if acc.pages == 0 {
                continue;
            }
            let decoded = acc.values * 8;
            let padded = acc.padded_values * 8;
            columns.push(IntegerColumn {
                column: name,
                column_id,
                pages: acc.pages,
                values: acc.values,
                padded_values: acc.padded_values,
                current_bytes: acc.current_bytes,
                current_encodings: acc.encodings,
                current_decode_mb_per_s: mb_per_s(decoded, acc.current_secs),
                fastlanes_for_bytes: acc.for_bytes,
                fastlanes_for_enveloped_bytes: acc.for_enveloped,
                fastlanes_for_decode_mb_per_s: mb_per_s(decoded, acc.for_secs),
                fastlanes_for_padded_decode_mb_per_s: mb_per_s(padded, acc.for_secs),
                fastlanes_delta_bytes: acc.delta_bytes,
                fastlanes_delta_enveloped_bytes: acc.delta_enveloped,
                fastlanes_delta_decode_mb_per_s: mb_per_s(decoded, acc.delta_secs),
                fastlanes_delta_padded_decode_mb_per_s: mb_per_s(padded, acc.delta_secs),
            });
        }

        Report {
            stamp,
            substring: SubstringSection {
                word_bloom_rule: "Single-token needles only (ASCII alphanumeric, one tokenizer \
                                  token): kept_blocks is the production scan's \
                                  blocks_after_bloom for HasWord on body; word_match_blocks \
                                  counts blocks where some body has the needle as a whole token. \
                                  A word match is not a substring match: a block holding the \
                                  needle only inside a longer token is not a word match.",
                needles,
            },
            trigram_sizing,
            body_encoding,
            integer_pages: IntegerSection {
                layout: fastlanes_layout(),
                columns,
                excluded,
                per_page: self.int_pages,
            },
        }
    }
}

fn fastlanes_layout() -> &'static str {
    "1024-value vectors, 16 lanes of 64 rows (value j*16+l in lane l), each lane bit-packed \
     LSB-first at the vector's width with lane words interleaved; scalar code. fastlanes_for: \
     per vector an i64 base and u8 width, then value-minus-base offsets. fastlanes_delta: per \
     vector the 16 first-row values, then the frame of reference of each lane's row-to-row \
     deltas. A page is a u32 count then its vectors; a short last vector is padded and counted. \
     *_bytes is raw; *_enveloped_bytes is after the production zstd envelope. Current bytes are \
     PAGE_DIR's stored page lengths, which the report re-derives with the production encoders \
     and fails on any difference."
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    const BLOCK: usize = 64;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/logbody")
            .join(name)
    }

    fn fixture_lines() -> Vec<Vec<u8>> {
        let text = std::fs::read(fixture("corpus.log")).unwrap();
        let mut lines: Vec<Vec<u8>> = text.split(|&b| b == b'\n').map(<[u8]>::to_vec).collect();
        if lines.last().is_some_and(Vec::is_empty) {
            lines.pop();
        }
        lines
    }

    fn fixture_report() -> Report {
        let mut cfg = Config::new(
            fixture("corpus.log"),
            fixture("needles.txt"),
            "2026-10-14".to_string(),
            "test".to_string(),
        );
        cfg.block_records = BLOCK;
        cfg.object_records = BLOCK * 32;
        cfg.decode_runs = 1;
        run(&cfg).expect("report runs")
    }

    #[test]
    fn fixture_has_the_exact_block_count() {
        let lines = fixture_lines();
        assert_eq!(lines.len(), 400);
        let r = fixture_report();
        assert_eq!(r.stamp.line_count, 400);
        // 400 lines at 64 records per block: six full blocks and one of 16.
        assert_eq!(r.stamp.blocks, 7);
        assert_eq!(r.body_encoding.pages, 7);
        for n in &r.substring.needles {
            assert_eq!(n.total_blocks, 7, "{}", n.needle);
        }
    }

    /// Expected surviving blocks for `needle`, from the corpus lines alone.
    fn independent_blocks(lines: &[Vec<u8>], needle: &[u8]) -> (u64, u64) {
        let mut blocks = 0;
        let mut matched_lines = 0;
        for chunk in lines.chunks(BLOCK) {
            let n = chunk
                .iter()
                .filter(|l| l.windows(needle.len()).any(|w| w == needle))
                .count() as u64;
            matched_lines += n;
            blocks += u64::from(n > 0);
        }
        (blocks, matched_lines)
    }

    #[test]
    fn block_survival_matches_an_independent_scan() {
        let lines = fixture_lines();
        let r = fixture_report();
        let find = |s: &str| {
            r.substring
                .needles
                .iter()
                .find(|n| n.needle == s)
                .unwrap_or_else(|| panic!("needle {s} missing"))
        };
        // Needles chosen so that blocks and lines differ: a line count in
        // place of a block count cannot pass. The literals are the fixture's
        // own counts; the independent scan must agree with them.
        for (needle, want_blocks, want_lines) in [
            ("timeout", 3, 23),
            ("a7f3", 2, 7),
            ("/var/lib/app", 2, 6),
            ("DataNode", 3, 15),
        ] {
            let (blocks, lines_hit) = independent_blocks(&lines, needle.as_bytes());
            assert_eq!((blocks, lines_hit), (want_blocks, want_lines), "{needle}");
            let n = find(needle);
            assert_eq!(n.exact_match_blocks, blocks, "{needle}");
            assert_eq!(n.exact_match_lines, lines_hit, "{needle}");
            for t in &n.trigram_bloom {
                assert!(t.kept_blocks >= blocks, "{needle}: filter dropped a block");
            }
        }
        // The template-constant needle is in every block.
        let every = find("Received block");
        assert_eq!(every.exact_match_blocks, 7);
        assert_eq!(
            every.word_bloom,
            WordBloom::NotApplicable(NOT_A_SINGLE_TOKEN)
        );
        // A whole-token needle: the production Bloom keeps every block that
        // holds the token.
        match find("timeout").word_bloom {
            WordBloom::Kept {
                kept_blocks,
                word_match_blocks,
            } => {
                assert!(word_match_blocks > 0);
                assert!(kept_blocks >= word_match_blocks);
            }
            WordBloom::NotApplicable(_) => panic!("timeout is a single token"),
        }
        // A single token that occurs only inside longer tokens: a substring
        // match in two blocks, a word match in none.
        assert_eq!(
            find("a7f3").word_bloom,
            WordBloom::Kept {
                kept_blocks: 0,
                word_match_blocks: 0
            }
        );
    }

    #[test]
    fn template_split_rebuilds_every_fixture_line() {
        let lines = fixture_lines();
        // Preconditions: the fixture carries what a lossy split would drop.
        assert!(lines.iter().any(|l| l.windows(3).any(|w| w == b" 00")));
        assert!(lines.iter().any(|l| l.windows(2).any(|w| w == b"  ")));
        for (i, line) in lines.iter().enumerate() {
            let s = template::split(line);
            assert_eq!(
                template::reconstruct(&s.template, &s.vars).as_deref(),
                Some(line.as_slice()),
                "line {}: {}",
                i + 1,
                String::from_utf8_lossy(line)
            );
        }
        let r = fixture_report();
        assert_eq!(r.body_encoding.lines_checked, 400);
        assert_eq!(r.body_encoding.mismatch_count, 0);
        assert_eq!(r.body_encoding.template_count_corpus, 16);
    }

    #[test]
    fn report_has_every_section_and_stamp_field() {
        let r = fixture_report();
        assert!(r.problems().is_empty(), "{:?}", r.problems());
        let v = serde_json::to_value(&r).unwrap();
        let keys = |o: &serde_json::Value| -> Vec<String> {
            let mut k: Vec<String> = o.as_object().unwrap().keys().cloned().collect();
            k.sort();
            k
        };
        let mut sections = [
            "stamp",
            "substring",
            "trigram_sizing",
            "body_encoding",
            "integer_pages",
        ];
        sections.sort_unstable();
        assert_eq!(keys(&v), sections);
        for section in sections {
            let s = &v[section];
            let empty = s.as_array().is_some_and(Vec::is_empty)
                || s.as_object().is_some_and(serde_json::Map::is_empty);
            assert!(!empty, "{section} is empty");
        }
        let stamp = &v["stamp"];
        let mut stamp_fields = [
            "corpus_path",
            "line_count",
            "byte_count",
            "sha256",
            "body_bytes",
            "git_sha",
            "cpu_model",
            "date",
            "block_target_records",
            "block_max_bytes",
            "group_target_blocks",
            "zstd_level",
            "object_records",
            "objects",
            "blocks",
            "compressed_object_bytes",
            "lines_lossy_utf8",
            "decode_runs",
            "clock",
            "line_rule",
            "needle_rule",
            "record_rule",
        ];
        stamp_fields.sort_unstable();
        assert_eq!(keys(stamp), stamp_fields);
        assert_eq!(
            stamp["corpus_path"],
            fixture("corpus.log").display().to_string()
        );
        assert_eq!(stamp["git_sha"], "test");
        assert_eq!(stamp["date"], "2026-10-14");
        assert_eq!(stamp["block_target_records"], BLOCK);
        assert_eq!(stamp["blocks"], 7);
        assert_eq!(stamp["sha256"].as_str().unwrap().len(), 64);
        assert_eq!(
            stamp["body_bytes"].as_u64().unwrap(),
            fixture_lines().iter().map(|l| l.len() as u64).sum::<u64>()
        );
        assert_eq!(
            stamp["byte_count"].as_u64().unwrap(),
            std::fs::metadata(fixture("corpus.log")).unwrap().len()
        );
        assert_eq!(v["trigram_sizing"].as_array().unwrap().len(), 2);
        let cols: Vec<&str> = v["integer_pages"]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["column"].as_str().unwrap())
            .collect();
        assert_eq!(cols, ["ts", "stream_ref", "severity_num", "flags"]);
        assert_eq!(v["integer_pages"]["excluded"][0]["column"], "observed_ts");
        let mut per_page_fields = [
            "block",
            "column",
            "values",
            "padded_values",
            "current_enc",
            "current_bytes",
            "current_decode_mb_per_s",
            "fastlanes_for_bytes",
            "fastlanes_for_enveloped_bytes",
            "fastlanes_for_decode_mb_per_s",
            "fastlanes_for_padded_decode_mb_per_s",
            "fastlanes_delta_bytes",
            "fastlanes_delta_enveloped_bytes",
            "fastlanes_delta_decode_mb_per_s",
            "fastlanes_delta_padded_decode_mb_per_s",
        ];
        per_page_fields.sort_unstable();
        assert_eq!(keys(&v["integer_pages"]["per_page"][0]), per_page_fields);
    }

    #[test]
    fn padded_throughput_divides_by_the_values_fastlanes_unpacks() {
        // BLOCK records per block is under one vector, so every page is padded.
        let r = fixture_report();
        let mut pages = 0;
        for p in &r.integer_pages.per_page {
            assert!(p.values > 0 && p.values <= BLOCK as u64);
            assert_eq!(p.padded_values, 1024, "block {} {}", p.block, p.column);
            let scale = p.padded_values as f64 / p.values as f64;
            for (plain, padded) in [
                (
                    p.fastlanes_for_decode_mb_per_s,
                    p.fastlanes_for_padded_decode_mb_per_s,
                ),
                (
                    p.fastlanes_delta_decode_mb_per_s,
                    p.fastlanes_delta_padded_decode_mb_per_s,
                ),
            ] {
                assert!(
                    (padded / plain - scale).abs() < 1e-9 * scale,
                    "{plain} {padded}"
                );
            }
            pages += 1;
        }
        assert_eq!(pages, 28);
        for c in &r.integer_pages.columns {
            assert_eq!(c.padded_values, c.pages * 1024, "{}", c.column);
        }
    }

    #[test]
    fn a_mismatch_or_an_empty_section_is_a_problem() {
        let mut r = fixture_report();
        assert!(r.problems().is_empty());
        r.body_encoding.mismatch_count = 1;
        assert_eq!(r.problems().len(), 1);
        r.body_encoding.mismatch_count = 0;
        r.substring.needles.clear();
        assert_eq!(r.problems(), ["substring: no needles"]);
        let mut r = fixture_report();
        r.integer_pages.per_page.clear();
        assert_eq!(r.problems(), ["integer_pages: no integer pages"]);
    }

    #[test]
    fn gzip_corpus_and_max_lines() {
        use std::io::Write;
        let plain = std::fs::read(fixture("corpus.log")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let gz = dir.path().join("corpus.log.gz");
        let mut enc = flate2::write::GzEncoder::new(
            File::create(&gz).unwrap(),
            flate2::Compression::default(),
        );
        enc.write_all(&plain).unwrap();
        enc.finish().unwrap();

        let mut cfg = Config::new(
            gz.clone(),
            fixture("needles.txt"),
            "2026-10-14".to_string(),
            "test".to_string(),
        );
        cfg.block_records = BLOCK;
        cfg.decode_runs = 1;
        let r = run(&cfg).unwrap();
        let plain_report = fixture_report();
        assert_eq!(r.stamp.line_count, 400);
        assert_eq!(r.stamp.body_bytes, plain_report.stamp.body_bytes);
        assert_eq!(r.stamp.byte_count, std::fs::metadata(&gz).unwrap().len());
        assert_ne!(r.stamp.sha256, plain_report.stamp.sha256);

        cfg.max_lines = Some(100);
        let r = run(&cfg).unwrap();
        assert_eq!(r.stamp.line_count, 100);
        // 64 + 36 records.
        assert_eq!(r.stamp.blocks, 2);
    }

    #[test]
    fn single_token_rule() {
        assert_eq!(single_token(b"Timeout"), Some(b"timeout".to_vec()));
        assert_eq!(single_token(b"a7f3"), Some(b"a7f3".to_vec()));
        assert_eq!(single_token(b"Received block"), None);
        assert_eq!(single_token(b"/var"), None);
        assert_eq!(single_token(b""), None);
    }

    #[test]
    fn line_rule_drops_only_the_ending() {
        let mut input: &[u8] = b"a\r\nb\n\nc  \rd";
        let mut buf = Vec::new();
        let mut got = Vec::new();
        while next_line(&mut input, &mut buf).unwrap().is_some() {
            got.push(buf.clone());
        }
        assert_eq!(
            got,
            [b"a".to_vec(), b"b".to_vec(), Vec::new(), b"c  \rd".to_vec()]
        );
    }

    #[test]
    fn timestamps_strictly_increase() {
        for i in 0..10_000 {
            assert!(ts_of(i) < ts_of(i + 1));
        }
    }

    #[test]
    fn zero_decode_runs_is_rejected() {
        let mut cfg = Config::new(
            fixture("corpus.log"),
            fixture("needles.txt"),
            "2026-10-14".to_string(),
            "test".to_string(),
        );
        cfg.block_records = BLOCK;
        cfg.decode_runs = 0;
        let err = run(&cfg).expect_err("decode_runs=0 must be rejected");
        assert!(err.to_string().contains("decode_runs"), "{err}");
    }

    /// The pre-fix behaviour, reproduced only to prove the fix matters: `at`
    /// advances solely from `template::placeholder_count` on the decoded
    /// dictionary entry, inside an `and_then` chain that one corrupted row's
    /// id or dictionary lookup short-circuits, so a row after it reads the
    /// wrong variable window.
    fn rebuild_mismatches_pre_fix(
        got_ids: &[i64],
        got_dict: &[Vec<u8>],
        got_vars: &[Vec<u8>],
        bodies: &[&[u8]],
    ) -> u64 {
        let mut at = 0usize;
        let mut mismatches = 0u64;
        for (row, body) in bodies.iter().enumerate() {
            let rebuilt = got_ids
                .get(row)
                .and_then(|&id| got_dict.get(usize::try_from(id).ok()?))
                .and_then(|t| {
                    let n = template::placeholder_count(t);
                    let v = got_vars.get(at..at + n)?;
                    at += n;
                    template::reconstruct(t, v)
                });
            mismatches += u64::from(rebuilt.as_deref() != Some(*body));
        }
        mismatches
    }

    #[test]
    fn one_corrupted_row_does_not_cascade_into_later_rows() {
        // Two one-placeholder templates; three rows, each rebuildable from
        // its own id, dict entry, and variable. Row 1's id is corrupted
        // (out of range), so row 1 must mismatch on its own -- but rows 0
        // and 2 must still rebuild.
        let dict: Vec<Vec<u8>> = vec![b"A \0 end".to_vec(), b"B \0 end".to_vec()];
        let splits = vec![
            template::Split {
                template: dict[0].clone(),
                vars: vec![b"1".as_slice()],
            },
            template::Split {
                template: dict[1].clone(),
                vars: vec![b"2".as_slice()],
            },
            template::Split {
                template: dict[0].clone(),
                vars: vec![b"3".as_slice()],
            },
        ];
        let got_ids: Vec<i64> = vec![0, 99, 0]; // row 1's id is out of range.
        let got_vars: Vec<Vec<u8>> = vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()];
        let bodies: Vec<&[u8]> = vec![b"A 1 end", b"B 2 end", b"A 3 end"];

        let fixed = rebuild_mismatches(&splits, &got_ids, &dict, &got_vars, &bodies);
        assert_eq!(fixed, 1, "only row 1's own corruption should mismatch");

        let pre_fix = rebuild_mismatches_pre_fix(&got_ids, &dict, &got_vars, &bodies);
        assert_eq!(
            pre_fix, 2,
            "pre-fix code lets row 1's failure desync row 2's variable window too"
        );
    }
}
