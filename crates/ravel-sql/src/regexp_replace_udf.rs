//! `regexp_replace(str, regexp, replacement[, flags])`, registered in place of
//! DataFusion's built-in so a constant pattern is compiled once per session
//! instead of once per record batch (epic #3231).
//!
//! DataFusion 55.2's built-in compiles the pattern, and for the
//! `^...(x)....*$` / `\1` shape a shortened copy of it, on every invocation,
//! and an invocation is one record batch. [`RegexpReplaceUdf`] keeps the
//! compiled state in a cache owned by the UDF instance, keyed by the pattern
//! and flags as written in the statement (`tests::one_scalar_pattern_compiles_once_across_batches`,
//! `tests::flags_are_part_of_the_cache_key`). `build_session_with_tables`
//! registers one instance per session, so the cache lives for one statement
//! (`session::tests::regexp_replace_is_the_ravel_udf_and_matches_the_builtin`).
//!
//! When the pattern, the replacement and the flags (if given) are all
//! non-dictionary string scalars and the source is `Utf8`, `LargeUtf8` or
//! `Utf8View`, the per-row evaluation below is a port of the built-in's
//! `regexp_replace_static_pattern_replace`: the same flag embedding and limit,
//! the same POSIX group conversion of the replacement, the same short-extract
//! fast path and its fallbacks, and the same null handling. Every other
//! argument shape is passed to the built-in unchanged. `tests::differential`
//! runs both on the same generated inputs and asserts identical results.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, PoisonError};

use datafusion::arrow::array::{
    Array, ArrayDataBuilder, ArrayRef, BufferBuilder, GenericStringArray, OffsetSizeTrait,
    StringViewBuilder, new_null_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::{as_generic_string_array, as_string_view_array};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::scalar::ScalarValue;
use regex::{CaptureLocations, Regex};

/// Distinct (pattern, flags) pairs one instance keeps; the cache is cleared
/// when an insert would exceed it (`tests::cache_is_bounded`).
const CACHE_CAPACITY: usize = 64;

/// Build the `regexp_replace` scalar UDF.
pub(crate) fn regexp_replace_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpReplaceUdf::new())
}

/// The pattern and flags exactly as the statement gave them.
type CacheKey = (String, Option<String>);

pub(crate) struct RegexpReplaceUdf {
    /// DataFusion's own `regexp_replace`: the source of the signature, the
    /// return type and the documentation, and the evaluator for every argument
    /// shape the cached path does not take.
    builtin: Arc<ScalarUDF>,
    cache: Mutex<HashMap<CacheKey, Arc<CompiledPattern>>>,
    full_compiles: AtomicUsize,
    short_compiles: AtomicUsize,
}

/// Everything the built-in derives from (pattern, flags) before its row loop.
struct CompiledPattern {
    re: Regex,
    /// The pattern with the flags embedded, as the built-in compiles it.
    pattern: String,
    /// 0 replaces every match, 1 the first only.
    limit: usize,
    /// The short-extract regex, built on the first call whose replacement makes
    /// the built-in try one. Its value depends on `pattern` alone.
    short: OnceLock<Option<Regex>>,
}

impl RegexpReplaceUdf {
    fn new() -> Self {
        RegexpReplaceUdf {
            builtin: datafusion::functions::regex::regexp_replace(),
            cache: Mutex::new(HashMap::new()),
            full_compiles: AtomicUsize::new(0),
            short_compiles: AtomicUsize::new(0),
        }
    }

    /// The cached state for (pattern, flags), compiling it on a miss. A pattern
    /// that does not compile returns the built-in's error and caches nothing.
    fn compiled(&self, pattern: &str, flags: Option<&str>) -> DFResult<Arc<CompiledPattern>> {
        let key = (pattern.to_owned(), flags.map(str::to_owned));
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(hit) = cache.get(&key) {
            return Ok(Arc::clone(hit));
        }
        let (pattern, limit) = embed_flags(pattern, flags);
        self.full_compiles.fetch_add(1, Ordering::Relaxed);
        let re = Regex::new(&pattern).map_err(|err| DataFusionError::External(Box::new(err)))?;
        if cache.len() >= CACHE_CAPACITY {
            cache.clear();
        }
        let entry = Arc::new(CompiledPattern {
            re,
            pattern,
            limit,
            short: OnceLock::new(),
        });
        cache.insert(key, Arc::clone(&entry));
        Ok(entry)
    }

    fn short_extract<'a>(
        &self,
        compiled: &'a CompiledPattern,
        replacement: &str,
    ) -> Option<&'a Regex> {
        if compiled.limit != 1 || replacement != "${1}" {
            return None;
        }
        compiled
            .short
            .get_or_init(|| {
                build_short_extract_regex(&compiled.pattern, || {
                    self.short_compiles.fetch_add(1, Ordering::Relaxed);
                })
            })
            .as_ref()
    }

    #[cfg(test)]
    fn compile_counts(&self) -> (usize, usize) {
        (
            self.full_compiles.load(Ordering::Relaxed),
            self.short_compiles.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    fn cache_len(&self) -> usize {
        self.cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl fmt::Debug for RegexpReplaceUdf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegexpReplaceUdf").finish_non_exhaustive()
    }
}

// The cache and counters are evaluation state, not part of the function's
// identity: every instance computes the same function.
impl PartialEq for RegexpReplaceUdf {
    fn eq(&self, other: &Self) -> bool {
        self.builtin == other.builtin
    }
}

impl Eq for RegexpReplaceUdf {}

impl Hash for RegexpReplaceUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.builtin.hash(state);
    }
}

impl ScalarUDFImpl for RegexpReplaceUdf {
    fn name(&self) -> &str {
        self.builtin.name()
    }

    fn signature(&self) -> &Signature {
        self.builtin.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.builtin.inner().return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let Some(constants) = Constants::read(&args.args) else {
            return self.builtin.invoke_with_args(args);
        };
        let source = &args.args[0];
        let source_is_scalar = matches!(source, ColumnarValue::Scalar(_));
        let source = source.to_array(1)?;
        let result = self.replace_static(&source, &constants)?;
        if source_is_scalar {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.builtin.inner().documentation()
    }
}

/// The scalar pattern, replacement and flags of a call the cached path takes.
/// `None` in a value is a NULL scalar, kept with its type because the built-in
/// returns a null array of that type.
struct Constants<'a> {
    pattern: (Option<&'a str>, DataType),
    replacement: (Option<&'a str>, DataType),
    flags: Option<(Option<&'a str>, DataType)>,
}

impl<'a> Constants<'a> {
    fn read(args: &'a [ColumnarValue]) -> Option<Self> {
        if !matches!(args.len(), 3 | 4)
            || !matches!(
                args[0].data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        {
            return None;
        }
        let pattern = string_scalar(&args[1])?;
        let replacement = string_scalar(&args[2])?;
        let flags = match args.get(3) {
            Some(flags) => Some(string_scalar(flags)?),
            None => None,
        };
        Some(Constants {
            pattern,
            replacement,
            flags,
        })
    }
}

fn string_scalar(arg: &ColumnarValue) -> Option<(Option<&str>, DataType)> {
    match arg {
        ColumnarValue::Scalar(
            value @ (ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s)),
        ) => Some((s.as_deref(), value.data_type())),
        _ => None,
    }
}

impl RegexpReplaceUdf {
    /// The built-in's `regexp_replace_static_pattern_replace`, with the
    /// compiled state taken from the cache.
    fn replace_static(&self, source: &ArrayRef, constants: &Constants<'_>) -> DFResult<ArrayRef> {
        let array_size = source.len();
        // A NULL pattern, replacement or flags makes every row NULL, checked in
        // the built-in's order and before anything is compiled.
        let (Some(pattern), _) = constants.pattern else {
            return Ok(new_null_array(&constants.pattern.1, array_size));
        };
        let (Some(replacement), _) = constants.replacement else {
            return Ok(new_null_array(&constants.replacement.1, array_size));
        };
        let flags = match &constants.flags {
            None => None,
            Some((Some(flags), _)) => Some(*flags),
            Some((None, data_type)) => return Ok(new_null_array(data_type, array_size)),
        };

        let compiled = self.compiled(pattern, flags)?;
        let replacement = regex_replace_posix_groups(replacement);
        // A clone shares the compiled program but gets its own search-cache
        // pool, so each partition's invocation searches on its own pool instead
        // of every thread but one contending for the cached regex's pool per row.
        let re = compiled.re.clone();
        let short = self.short_extract(&compiled, &replacement).cloned();
        let mut replacer = Replacer {
            re: &re,
            short: short
                .as_ref()
                .map(|short| (short, short.capture_locations())),
            limit: compiled.limit,
            replacement: &replacement,
        };

        match source.data_type() {
            DataType::Utf8 => replace_offsets::<i32>(source, &mut replacer),
            DataType::LargeUtf8 => replace_offsets::<i64>(source, &mut replacer),
            DataType::Utf8View => {
                let strings = as_string_view_array(source)?;
                let mut builder = StringViewBuilder::with_capacity(strings.len());
                for val in strings.iter() {
                    match val {
                        Some(val) => builder.append_value(replacer.replacen(val).as_ref()),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function regexp_replace"
            ))),
        }
    }
}

/// The built-in's `Utf8`/`LargeUtf8` loop: the source's null buffer is reused
/// and a null row contributes no bytes.
fn replace_offsets<T: OffsetSizeTrait>(
    source: &ArrayRef,
    replacer: &mut Replacer<'_>,
) -> DFResult<ArrayRef> {
    let strings = as_generic_string_array::<T>(source)?;
    let offsets = strings.value_offsets();
    let mut vals = BufferBuilder::<u8>::new((offsets[strings.len()] - offsets[0]).as_usize());
    let mut new_offsets = BufferBuilder::<T>::new(strings.len() + 1);
    new_offsets.append(T::zero());
    for val in strings.iter() {
        if let Some(val) = val {
            vals.append_slice(replacer.replacen(val).as_bytes());
        }
        let end = T::from_usize(vals.len()).ok_or_else(|| {
            DataFusionError::Execution("regexp_replace result exceeds the offset range".into())
        })?;
        new_offsets.append(end);
    }
    let data = ArrayDataBuilder::new(GenericStringArray::<T>::DATA_TYPE)
        .len(strings.len())
        .nulls(strings.nulls().cloned())
        .buffers(vec![new_offsets.finish(), vals.finish()])
        .build()?;
    Ok(Arc::new(GenericStringArray::<T>::from(data)) as ArrayRef)
}

/// Embed the flags into the pattern as the built-in does; the limit is 0 (every
/// match) when the flags contain `g` and 1 otherwise.
fn embed_flags(pattern: &str, flags: Option<&str>) -> (String, usize) {
    match flags {
        Some("g") => (pattern.to_owned(), 0),
        Some(flags) => (
            format!("(?{}){}", flags.replace('g', ""), pattern),
            usize::from(!flags.contains('g')),
        ),
        None => (pattern.to_owned(), 1),
    }
}

/// Convert POSIX groups (`\1`, `\\1`) to the regex crate's `${1}`, as the
/// built-in does.
fn regex_replace_posix_groups(replacement: &str) -> String {
    static CAPTURE_GROUPS: LazyLock<Option<Regex>> =
        LazyLock::new(|| Regex::new(r"\\{1,2}(\d+)").ok());
    match CAPTURE_GROUPS.as_ref() {
        Some(re) => re.replace_all(replacement, "$${$1}").into_owned(),
        None => replacement.to_owned(),
    }
}

/// The built-in's `try_build_short_extract_regex` after its replacement check:
/// for `^...(x)....*$`, the pattern without its trailing `.*$`, kept only when
/// it compiles with exactly one capture group. `on_compile` runs before each
/// compile attempt.
fn build_short_extract_regex(pattern: &str, on_compile: impl FnOnce()) -> Option<Regex> {
    if !pattern.starts_with('^') || !pattern.ends_with(".*$") {
        return None;
    }
    on_compile();
    let re = Regex::new(&pattern[..pattern.len() - 3]).ok()?;
    (re.captures_len() == 2).then_some(re)
}

/// The built-in's `OptimizedRegex` for one invocation, over that invocation's
/// clones of the cached regexes; the capture locations are per invocation too.
struct Replacer<'a> {
    re: &'a Regex,
    short: Option<(&'a Regex, CaptureLocations)>,
    limit: usize,
    replacement: &'a str,
}

impl Replacer<'_> {
    fn replacen<'v>(&mut self, val: &'v str) -> Cow<'v, str> {
        let Some((short, locs)) = self.short.as_mut() else {
            return self.re.replacen(val, self.limit, self.replacement);
        };
        // The anchored full pattern cannot match where its prefix does not, so
        // the row is returned unchanged.
        if short.captures_read(locs, val).is_none() {
            return Cow::Borrowed(val);
        }
        // `.*$` does not cross a newline, so a newline after the prefix match
        // goes to the full regex.
        let Some((_, match_end)) = locs.get(0) else {
            return self.re.replacen(val, self.limit, self.replacement);
        };
        if val.as_bytes()[match_end..].contains(&b'\n') {
            return self.re.replacen(val, self.limit, self.replacement);
        }
        match locs.get(1) {
            Some((start, end)) => Cow::Borrowed(&val[start..end]),
            None => Cow::Borrowed(""),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, LargeStringArray, StringArray, StringViewArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::config::ConfigOptions;
    use datafusion::error::Result as DFResult;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
    use datafusion::scalar::ScalarValue;
    use proptest::prelude::*;

    use super::{CACHE_CAPACITY, RegexpReplaceUdf};

    const Q29: &str = r"^https?://(?:www\.)?([^/]+)/.*$";

    fn call(udf: &ScalarUDF, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let types: Vec<DataType> = args.iter().map(ColumnarValue::data_type).collect();
        let number_rows = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let return_type = udf.return_type(&types)?;
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: types
                .iter()
                .enumerate()
                .map(|(i, t)| Arc::new(Field::new(format!("a{i}"), t.clone(), true)))
                .collect(),
            number_rows,
            return_field: Arc::new(Field::new("r", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    /// Both results as comparable text: the variant, the type and the values,
    /// or the error's message.
    fn render(result: DFResult<ColumnarValue>) -> String {
        match result {
            Ok(ColumnarValue::Scalar(value)) => {
                format!("scalar {:?} {value:?}", value.data_type())
            }
            Ok(ColumnarValue::Array(array)) => {
                let rows: Vec<String> = (0..array.len())
                    .map(|i| match ScalarValue::try_from_array(&array, i) {
                        Ok(value) => format!("{value:?}"),
                        Err(err) => format!("row error {err}"),
                    })
                    .collect();
                format!("array {:?} {rows:?}", array.data_type())
            }
            Err(err) => format!("error {err}"),
        }
    }

    fn assert_same(ravel: &ScalarUDF, args: &[ColumnarValue]) {
        let builtin = datafusion::functions::regex::regexp_replace();
        let expected = call(&builtin, args);
        let actual = call(ravel, args);
        if let (Ok(ColumnarValue::Array(e)), Ok(ColumnarValue::Array(a))) = (&expected, &actual) {
            assert_eq!(e.to_data(), a.to_data(), "args {args:?}");
        }
        assert_eq!(render(actual), render(expected), "args {args:?}");
    }

    fn string_array(kind: u8, values: &[Option<&str>]) -> ArrayRef {
        match kind {
            0 => Arc::new(StringArray::from(values.to_vec())),
            1 => Arc::new(LargeStringArray::from(values.to_vec())),
            _ => Arc::new(StringViewArray::from(values.to_vec())),
        }
    }

    fn string_scalar(kind: u8, value: Option<&str>) -> ScalarValue {
        let value = value.map(str::to_owned);
        match kind {
            0 => ScalarValue::Utf8(value),
            1 => ScalarValue::LargeUtf8(value),
            _ => ScalarValue::Utf8View(value),
        }
    }

    fn scalar_args(
        kind: u8,
        source: &[Option<&str>],
        pattern: &str,
        replacement: &str,
        flags: Option<&str>,
    ) -> Vec<ColumnarValue> {
        let mut args = vec![
            ColumnarValue::Array(string_array(kind, source)),
            ColumnarValue::Scalar(string_scalar(kind, Some(pattern))),
            ColumnarValue::Scalar(string_scalar(kind, Some(replacement))),
        ];
        if let Some(flags) = flags {
            args.push(ColumnarValue::Scalar(string_scalar(kind, Some(flags))));
        }
        args
    }

    fn ravel() -> (ScalarUDF, Arc<RegexpReplaceUdf>) {
        let inner = Arc::new(RegexpReplaceUdf::new());
        (ScalarUDF::new_from_shared_impl(inner.clone()), inner)
    }

    const PATTERNS: &[&str] = &[
        Q29,
        r"^(a+)b.*$",
        r"^ab.*$",
        r"^(x)?y.*$",
        r"^(a)(b).*$",
        r"b(.)",
        r"a",
        r"(a)(b)?",
        r"[a-c]+",
        "",
        "(",
        "[",
    ];
    const REPLACEMENTS: &[&str] = &[
        "X", r"\1", r"\\1", "${1}", "<${0}>", "", r"\2", "$1x", r"\\\1",
    ];
    const FLAGS: &[Option<&str>] = &[None, Some("g"), Some("i"), Some("gi")];

    fn source_value() -> impl Strategy<Value = String> {
        prop_oneof![
            (
                prop::sample::select(vec!["http://", "https://www.", "https://", "ftp://", ""]),
                "[abcwx.]{0,4}",
                prop::sample::select(vec!["/", ""]),
                "[abxy/\n]{0,4}",
            )
                .prop_map(|(scheme, host, slash, tail)| format!("{scheme}{host}{slash}{tail}")),
            "[abcxyAB/:.\n]{0,8}",
        ]
    }

    #[derive(Debug, Clone)]
    struct Case {
        kind: u8,
        source: Vec<Option<String>>,
        scalar_source: bool,
        pattern: Option<&'static str>,
        replacement: Option<&'static str>,
        /// `None`: three arguments. `Some(None)`: a NULL flags argument.
        flags: Option<Option<&'static str>>,
        /// Pass the pattern as an array, which the built-in evaluates.
        array_pattern: bool,
    }

    fn case() -> impl Strategy<Value = Case> {
        (
            0u8..3,
            prop::collection::vec(prop::option::weighted(0.85, source_value()), 0..8),
            prop::bool::weighted(0.1),
            prop::option::weighted(0.95, prop::sample::select(PATTERNS)),
            prop::option::weighted(0.95, prop::sample::select(REPLACEMENTS)),
            prop_oneof![
                10 => prop::sample::select(FLAGS).prop_map(|flags| flags.map(Some)),
                1 => Just(Some(None)),
            ],
            prop::bool::weighted(0.2),
        )
            .prop_map(
                |(kind, source, scalar_source, pattern, replacement, flags, array_pattern)| Case {
                    kind,
                    source,
                    scalar_source,
                    pattern,
                    replacement,
                    flags,
                    array_pattern,
                },
            )
    }

    fn case_args(case: &Case) -> Vec<ColumnarValue> {
        let rows: Vec<Option<&str>> = case.source.iter().map(Option::as_deref).collect();
        let source = if case.scalar_source {
            ColumnarValue::Scalar(string_scalar(case.kind, rows.first().copied().flatten()))
        } else {
            ColumnarValue::Array(string_array(case.kind, &rows))
        };
        let pattern = if case.array_pattern && !case.scalar_source {
            ColumnarValue::Array(string_array(case.kind, &vec![case.pattern; rows.len()]))
        } else {
            ColumnarValue::Scalar(string_scalar(case.kind, case.pattern))
        };
        let mut args = vec![
            source,
            pattern,
            ColumnarValue::Scalar(string_scalar(case.kind, case.replacement)),
        ];
        if let Some(flags) = case.flags {
            args.push(ColumnarValue::Scalar(string_scalar(case.kind, flags)));
        }
        args
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// The Ravel UDF and DataFusion's built-in agree on every generated
        /// input, on a first invocation (cache miss) and a second one with
        /// different rows (cache hit).
        #[test]
        fn differential(first in case(), rows in prop::collection::vec(
            prop::option::weighted(0.85, source_value()), 0..8)) {
            let (udf, _) = ravel();
            assert_same(&udf, &case_args(&first));
            let second = Case { source: rows, ..first };
            assert_same(&udf, &case_args(&second));
        }
    }

    #[test]
    fn short_extract_returns_a_non_matching_row_unchanged() {
        let (udf, inner) = ravel();
        for kind in 0..3 {
            let source = [
                Some("https://www.example.com/path"),
                Some("ftp://example.com/path"),
                None,
                Some("http://a.b/x\ny"),
                Some("http://nohost"),
            ];
            let args = scalar_args(kind, &source, Q29, r"\1", None);
            assert_same(&udf, &args);
            let Ok(ColumnarValue::Array(out)) = call(&udf, &args) else {
                panic!("array result");
            };
            let out = datafusion::arrow::compute::cast(&out, &DataType::Utf8).expect("cast");
            let out = out
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 result");
            assert_eq!(out.value(0), "example.com");
            assert_eq!(out.value(1), "ftp://example.com/path");
            assert!(out.is_null(2));
        }
        assert_eq!(inner.compile_counts(), (1, 1));
    }

    #[test]
    fn one_scalar_pattern_compiles_once_across_batches() {
        let (udf, inner) = ravel();
        for batch in 0..5 {
            let row = format!("https://www.host{batch}.com/page");
            let args = scalar_args(0, &[Some(row.as_str()), None], Q29, r"\1", None);
            assert_same(&udf, &args);
        }
        assert_eq!(inner.compile_counts(), (1, 1));
    }

    #[test]
    fn two_scalar_patterns_compile_twice() {
        let (udf, inner) = ravel();
        for batch in 0..6 {
            let pattern = if batch % 2 == 0 { "b(.)" } else { "a+" };
            let args = scalar_args(2, &[Some("aabbcc"), Some("xyz")], pattern, "<${1}>", None);
            assert_same(&udf, &args);
        }
        assert_eq!(inner.compile_counts().0, 2);
    }

    #[test]
    fn flags_are_part_of_the_cache_key() {
        let (udf, inner) = ravel();
        let batches: Vec<_> = (0..4)
            .map(|batch| {
                let flags = if batch % 2 == 0 { None } else { Some("g") };
                scalar_args(0, &[Some("ababab")], "b", "X", flags)
            })
            .collect();
        for args in &batches {
            call(&udf, args).expect("replaces");
        }
        assert_eq!(inner.compile_counts().0, 2);
        for args in &batches {
            assert_same(&udf, args);
        }
    }

    #[test]
    fn invalid_pattern_errors_every_time_and_is_not_cached() {
        let (udf, inner) = ravel();
        for _ in 0..3 {
            let args = scalar_args(1, &[Some("abc")], "(", "X", Some("g"));
            assert!(call(&udf, &args).is_err());
            assert_same(&udf, &args);
        }
        // Each of the three loop turns invokes the UDF twice.
        assert_eq!(inner.compile_counts().0, 6);
        assert_eq!(inner.cache_len(), 0);
    }

    #[test]
    fn null_constants_return_nulls_before_compiling() {
        let (udf, inner) = ravel();
        let source = ColumnarValue::Array(string_array(0, &[Some("abc"), None]));
        let null = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let invalid = ColumnarValue::Scalar(ScalarValue::Utf8(Some("(".into())));
        let x = ColumnarValue::Scalar(ScalarValue::Utf8(Some("X".into())));
        assert_same(&udf, &[source.clone(), invalid.clone(), null.clone()]);
        assert_same(&udf, &[source.clone(), null.clone(), x.clone()]);
        assert_same(&udf, &[source, invalid, x, null]);
        assert_eq!(inner.compile_counts().0, 0);
    }

    #[test]
    fn cache_is_bounded() {
        let (udf, inner) = ravel();
        for i in 0..(CACHE_CAPACITY * 2 + 3) {
            let pattern = format!("a{{{i}}}");
            let args = scalar_args(0, &[Some("aaaa"), Some("b")], &pattern, "X", None);
            assert_same(&udf, &args);
            assert!(inner.cache_len() <= CACHE_CAPACITY);
        }
    }

    #[test]
    fn concurrent_invocations_share_one_compile() {
        let (udf, inner) = ravel();
        std::thread::scope(|scope| {
            for t in 0..8 {
                let udf = &udf;
                scope.spawn(move || {
                    let row = format!("http://h{t}.org/x");
                    let args = scalar_args(0, &[Some(row.as_str())], Q29, r"\\1", None);
                    assert_same(udf, &args);
                });
            }
        });
        assert_eq!(inner.compile_counts(), (1, 1));
    }

    #[test]
    fn identity_matches_across_instances() {
        let (a, _) = ravel();
        let (b, _) = ravel();
        assert_eq!(a, b);
        assert_eq!(a.name(), "regexp_replace");
        assert_eq!(
            a.signature(),
            datafusion::functions::regex::regexp_replace().signature()
        );
    }
}
