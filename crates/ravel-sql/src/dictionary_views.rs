//! A memoized `Dictionary(Int32, Utf8)` to `Utf8View` cast, the group-key
//! cast [`crate::DictionaryGroupKeysAsViews`] inserts (issue #3231).
//!
//! Arrow's cast builds one 16-byte view per row from the row's dictionary
//! entry, reading that entry's offsets and leading bytes at a position the
//! key picks. The scan shares one values array per row-group dictionary
//! across the batches it emits (`logs_scan::batch::StrValuesCache`), so that
//! per-row read repeats work an earlier batch already did.
//! [`DictionaryViewsCast`] builds the views of a values array once
//! ([`views_of`]) and then gathers each row's view by key, with the type,
//! nulls, view bits and data buffers arrow's cast produces
//! (`tests::matches_the_stock_cast`).
//!
//! A values array's views are built only once the rows cast over it, the
//! current batch's included, reach its entry count; until then each batch
//! takes arrow's cast ([`Memo::lookup`],
//! `tests::large_dictionary_builds_once_rows_reach_its_length`). A dictionary
//! with more entries than the rows that read it is never built.
//!
//! The memo belongs to the expression instance, which every partition of the
//! aggregate shares; its lock is taken once or twice per batch, never per
//! row. It keeps at most [`MEMO_ENTRIES`] values arrays (`tests::memo_is_bounded`).
//! A built entry's views hold the values' data buffer ([`views_of`] appends
//! it as block 0), so up to that many data buffers can outlive the scan's own
//! reference to them; the memory pool is not charged for them or for the
//! 16 bytes per entry the views take.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, StringArray, StringViewBuilder, UInt32Array,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Int32Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

/// How many values arrays one expression keeps an entry for, built or not.
/// The least recently used entry leaves first.
pub(crate) const MEMO_ENTRIES: usize = 4;

/// The type this cast reads with its memo; any other input takes the stock
/// cast.
pub(crate) fn is_memoized_input(ty: &DataType) -> bool {
    matches!(ty, DataType::Dictionary(key, value)
        if **key == DataType::Int32 && **value == DataType::Utf8)
}

/// `CAST(expr AS Utf8View)` for a `Dictionary(Int32, Utf8)` `expr`, memoizing
/// the views of each dictionary's values. See the module docs.
pub struct DictionaryViewsCast {
    expr: Arc<dyn PhysicalExpr>,
    memo: Mutex<Memo>,
}

impl DictionaryViewsCast {
    /// The cast of `expr` with an empty memo.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        DictionaryViewsCast {
            expr,
            memo: Mutex::new(Memo::default()),
        }
    }

    /// The expression whose value this casts.
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// How many views arrays this expression has built.
    #[cfg(test)]
    fn builds(&self) -> usize {
        self.memo
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .builds
    }

    fn cast(&self, array: &ArrayRef) -> DFResult<ArrayRef> {
        let stock = || {
            ColumnarValue::Array(Arc::clone(array))
                .cast_to(&DataType::Utf8View, None)?
                .into_array(array.len())
        };
        let Some(dict) = array.as_dictionary_opt::<Int32Type>() else {
            return stock();
        };
        let Some(values) = dict.values().as_string_opt::<i32>() else {
            return stock();
        };
        let rows = dict.len();
        if rows == 0 {
            // `take` answers no indices with a new empty array, without the
            // values buffer arrow's cast keeps.
            return stock();
        }
        // Bound first: a guard in the match scrutinee would live through the
        // arms, and the build arm locks again.
        let lookup = self.lock().lookup(dict.values(), rows);
        let views = match lookup {
            Lookup::Built(views) => views,
            Lookup::Stock => return stock(),
            Lookup::Build => {
                // Built outside the lock: another partition casting over a
                // different dictionary does not wait for this one. Two
                // partitions racing on one dictionary both build it, and the
                // memo keeps whichever lands first.
                let Ok(views) = views_of(values) else {
                    self.lock().refuse(dict.values());
                    return stock();
                };
                self.lock().insert(dict.values(), views)
            }
        };
        let indices = gather_indices(dict.keys(), values.len())?;
        Ok(take(views.as_ref(), &indices, None)?)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Memo> {
        self.memo.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The view of every entry of `values`, followed by one null: the entry a
/// NULL key gathers. Each view is the one arrow's cast makes for that entry,
/// over `values`' own data buffer as block 0, read from the entry's offsets
/// whether or not the entry is NULL: arrow's cast takes its output's nulls
/// from the keys alone, so a NULL entry reads as its bytes, valid
/// (`dictionary_views::tests::matches_the_stock_cast` generates NULL entries
/// over non-empty bytes). An entry whose bytes are not UTF-8, which only a
/// NULL entry of an unchecked array can hold, is refused.
fn views_of(values: &StringArray) -> DFResult<ArrayRef> {
    let mut builder = StringViewBuilder::with_capacity(values.len() + 1);
    let block = builder.append_block(values.values().clone());
    for window in values.value_offsets().windows(2) {
        let [start, end] = window else {
            continue;
        };
        let start = u32::try_from(*start).map_err(offset_error)?;
        let end = u32::try_from(*end).map_err(offset_error)?;
        builder.try_append_view(block, start, end.saturating_sub(start))?;
    }
    builder.append_null();
    Ok(Arc::new(builder.finish()))
}

fn offset_error<E: fmt::Display>(err: E) -> DataFusionError {
    DataFusionError::Internal(format!("dictionary value offset out of range: {err}"))
}

/// Each row's index into the views [`views_of`] built for a dictionary of
/// `entries` values: its key, or `entries`, the trailing null, for a NULL key.
fn gather_indices(
    keys: &datafusion::arrow::array::Int32Array,
    entries: usize,
) -> DFResult<UInt32Array> {
    let null_entry = u32::try_from(entries).map_err(offset_error)?;
    let in_range = |key: i32| {
        u32::try_from(key)
            .ok()
            .filter(|&index| index < null_entry)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "dictionary key {key} outside its {entries} values"
                ))
            })
    };
    let indices = match keys.nulls().filter(|nulls| nulls.null_count() > 0) {
        Some(nulls) => keys
            .values()
            .iter()
            .zip(nulls.iter())
            .map(|(&key, valid)| if valid { in_range(key) } else { Ok(null_entry) })
            .collect::<DFResult<Vec<u32>>>()?,
        None => keys
            .values()
            .iter()
            .map(|&key| in_range(key))
            .collect::<DFResult<Vec<u32>>>()?,
    };
    Ok(UInt32Array::new(ScalarBuffer::from(indices), None))
}

/// The values arrays one expression has seen, most recently used last.
#[derive(Default)]
struct Memo {
    entries: Vec<Entry>,
    builds: usize,
}

struct Entry {
    /// The values array, found by its `Arc` identity ([`Memo::position`]).
    /// Only a `Weak`, so the memo does not count as a holder in
    /// `StrValuesCache`, which keeps an array while its strong count says
    /// something outside the cache holds it. The `Weak` keeps the allocation's
    /// address from being reused while the entry exists, so a live array at
    /// that address is this one.
    values: Weak<dyn Array>,
    state: State,
}

enum State {
    /// Rows the stock cast has converted over this values array so far.
    Pending(usize),
    Built(ArrayRef),
    /// [`views_of`] refused the values; every batch takes the stock cast.
    Refused,
}

enum Lookup {
    Built(ArrayRef),
    Build,
    Stock,
}

impl Memo {
    /// What to do for a batch of `rows` over `values`: gather from its built
    /// views, build them now, or take the stock cast. The views are built once
    /// the rows cast over `values`, this batch's included, reach its length.
    fn lookup(&mut self, values: &ArrayRef, rows: usize) -> Lookup {
        let entries = values.len();
        let Some(at) = self.position(values) else {
            if rows >= entries {
                return Lookup::Build;
            }
            self.push(values, State::Pending(rows));
            return Lookup::Stock;
        };
        let mut entry = self.entries.remove(at);
        let lookup = match &mut entry.state {
            State::Built(views) => Lookup::Built(Arc::clone(views)),
            State::Refused => Lookup::Stock,
            State::Pending(seen) => {
                *seen = seen.saturating_add(rows);
                if *seen >= entries {
                    Lookup::Build
                } else {
                    Lookup::Stock
                }
            }
        };
        self.entries.push(entry);
        lookup
    }

    /// Record `views`, built for `values`, and return the views to gather
    /// from: an entry another caller built first wins.
    fn insert(&mut self, values: &ArrayRef, views: ArrayRef) -> ArrayRef {
        if let Some(at) = self.position(values)
            && let State::Built(existing) = &self.entries[at].state
        {
            return Arc::clone(existing);
        }
        self.builds += 1;
        if let Some(at) = self.position(values) {
            self.entries.remove(at);
        }
        self.push(values, State::Built(Arc::clone(&views)));
        views
    }

    /// Record that `values` has no views array to build.
    fn refuse(&mut self, values: &ArrayRef) {
        if let Some(at) = self.position(values) {
            self.entries.remove(at);
        }
        self.push(values, State::Refused);
    }

    fn position(&self, values: &ArrayRef) -> Option<usize> {
        self.entries.iter().position(|entry| {
            std::ptr::addr_eq(entry.values.as_ptr(), Arc::as_ptr(values))
                && entry.values.strong_count() > 0
        })
    }

    fn push(&mut self, values: &ArrayRef, state: State) {
        self.entries.retain(|entry| entry.values.strong_count() > 0);
        if self.entries.len() >= MEMO_ENTRIES {
            self.entries.remove(0);
        }
        self.entries.push(Entry {
            values: Arc::downgrade(values),
            state,
        });
    }
}

impl fmt::Debug for DictionaryViewsCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DictionaryViewsCast")
            .field("expr", &self.expr)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for DictionaryViewsCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DICTIONARY_VIEWS({} AS Utf8View)", self.expr)
    }
}

impl PartialEq for DictionaryViewsCast {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
    }
}

impl Eq for DictionaryViewsCast {}

impl Hash for DictionaryViewsCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
    }
}

impl PhysicalExpr for DictionaryViewsCast {
    fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
        Ok(DataType::Utf8View)
    }

    fn nullable(&self, input_schema: &Schema) -> DFResult<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.cast(&array)?)),
            scalar @ ColumnarValue::Scalar(_) => scalar.cast_to(&DataType::Utf8View, None),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let [expr] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "DictionaryViewsCast takes one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(DictionaryViewsCast::new(expr)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST(")?;
        self.expr.fmt_sql(f)?;
        write!(f, " AS Utf8View)")
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{DictionaryArray, Int32Array, StringViewArray};
    use datafusion::arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;
    use proptest::prelude::*;

    fn dict_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    fn expr() -> DictionaryViewsCast {
        DictionaryViewsCast::new(Arc::new(Column::new("k", 0)))
    }

    fn batch(array: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).expect("batch builds")
    }

    fn eval(cast: &DictionaryViewsCast, array: &ArrayRef) -> ArrayRef {
        cast.evaluate(&batch(Arc::clone(array)))
            .expect("cast evaluates")
            .into_array(array.len())
            .expect("an array")
    }

    /// `actual` is the stock cast's output for `input`: the same type, the
    /// same value and validity per row, the same view bits (NULL rows'
    /// included), and the same data buffers, which are the dictionary values'
    /// own.
    #[track_caller]
    fn assert_stock(input: &ArrayRef, actual: &ArrayRef) {
        let expected = cast(input, &DataType::Utf8View).expect("stock cast");
        assert_eq!(actual.data_type(), expected.data_type());
        assert_eq!(actual.len(), expected.len());
        let actual = actual.as_string_view();
        let expected: &StringViewArray = expected.as_string_view();
        for row in 0..expected.len() {
            assert_eq!(actual.is_null(row), expected.is_null(row), "row {row}");
            if expected.is_valid(row) {
                assert_eq!(actual.value(row), expected.value(row), "row {row}");
            }
        }
        assert_eq!(actual.nulls().is_some(), expected.nulls().is_some());
        assert_eq!(actual.views(), expected.views());
        let pointers = |array: &StringViewArray| {
            array
                .data_buffers()
                .iter()
                .map(|b| (b.as_ptr(), b.len()))
                .collect::<Vec<_>>()
        };
        assert_eq!(pointers(actual), pointers(expected));
    }

    /// A dictionary over the non-empty `values` with `keys`, each key reduced
    /// modulo the values' length.
    fn dictionary(keys: &[Option<u8>], values: &ArrayRef) -> ArrayRef {
        let entries = i32::try_from(values.len()).expect("small");
        let keys: Int32Array = keys
            .iter()
            .map(|key| key.map(|k| i32::from(k) % entries))
            .collect();
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::clone(values)).expect("keys fit"))
    }

    /// An entry: whether it is valid, and its bytes, which a NULL entry
    /// keeps too.
    fn string() -> impl Strategy<Value = (bool, String)> {
        (
            prop::bool::weighted(0.8),
            prop_oneof!["[a-c]{0,12}", "[a-cé]{13,30}"],
        )
    }

    /// `entries` as a values array, NULL entries over their own bytes.
    fn values_array(entries: &[(bool, String)]) -> StringArray {
        let data: String = entries.iter().map(|(_, s)| s.as_str()).collect();
        let offsets = OffsetBuffer::from_lengths(entries.iter().map(|(_, s)| s.len()));
        let nulls = NullBuffer::from(entries.iter().map(|(valid, _)| *valid).collect::<Vec<_>>());
        StringArray::new(offsets, Buffer::from(data.into_bytes()), Some(nulls))
    }

    /// One dictionary batch: which values array it reads, its keys, and a
    /// slice taken of the dictionary afterwards.
    type Batch = (usize, Vec<Option<u8>>, usize, usize);

    /// Entries per values array, the values arrays, and the batches.
    type Case = (usize, Vec<Vec<(bool, String)>>, Vec<Batch>);

    fn case() -> impl Strategy<Value = Case> {
        (1usize..8).prop_flat_map(|entries| {
            (
                Just(entries),
                // Every values array has the same length, so a memo found by
                // length alone gathers from the wrong dictionary.
                prop::collection::vec(prop::collection::vec(string(), entries + 2), 1..4),
                prop::collection::vec(
                    (
                        0usize..4,
                        prop::collection::vec(prop::option::weighted(0.8, any::<u8>()), 0..20),
                        0usize..4,
                        0usize..4,
                    ),
                    1..10,
                ),
            )
        })
    }

    proptest! {
        /// Differential against arrow's cast over batches that share values
        /// arrays, so every path of the memo (stock while pending, the build,
        /// a hit) is compared, with NULL keys, NULL values, inline and
        /// out-of-line strings, sliced values and sliced dictionaries.
        #[test]
        fn matches_the_stock_cast((entries, dictionaries, batches) in case()) {
            let shared: Vec<ArrayRef> = dictionaries
                .iter()
                .map(|values| {
                    // Sliced by one entry, so the offsets do not start at 0.
                    Arc::new(values_array(values).slice(1, entries)) as ArrayRef
                })
                .collect();
            let cast = expr();
            for (pick, keys, from, drop) in &batches {
                let values = &shared[pick % shared.len()];
                let whole = dictionary(keys, values);
                let from = (*from).min(whole.len());
                let len = whole.len() - from - (*drop).min(whole.len() - from);
                let input = whole.slice(from, len);
                let actual = eval(&cast, &input);
                assert_stock(&input, &actual);
            }
        }
    }

    /// N batches sharing one values array build its views once; a batch over
    /// a different values array builds again, even at the same length.
    #[test]
    fn shared_values_build_their_views_once() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["x", "a long string past twelve"]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["y", "another string past twelve"]));
        let cast = expr();
        for _ in 0..5 {
            let input = dictionary(&[Some(1), None, Some(0)], &a);
            assert_stock(&input, &eval(&cast, &input));
        }
        assert_eq!(cast.builds(), 1);
        let input = dictionary(&[Some(0), Some(1)], &b);
        assert_stock(&input, &eval(&cast, &input));
        assert_eq!(cast.builds(), 2);
        let input = dictionary(&[Some(1)], &a);
        assert_stock(&input, &eval(&cast, &input));
        assert_eq!(cast.builds(), 2, "a is still memoized");
    }

    /// A values array longer than the batches over it is cast by the stock
    /// kernel until the rows cast over it reach its length, then built.
    #[test]
    fn large_dictionary_builds_once_rows_reach_its_length() {
        let values: ArrayRef = Arc::new(StringArray::from(
            (0..10).map(|i| format!("v{i}")).collect::<Vec<_>>(),
        ));
        let cast = expr();
        let input = dictionary(&[Some(3); 4], &values);
        assert_stock(&input, &eval(&cast, &input));
        assert_stock(&input, &eval(&cast, &input));
        assert_eq!(cast.builds(), 0, "8 rows of a 10-entry dictionary");
        assert_stock(&input, &eval(&cast, &input));
        assert_eq!(cast.builds(), 1, "12 rows reach the 10 entries");
        assert_stock(&input, &eval(&cast, &input));
        assert_eq!(cast.builds(), 1);
    }

    /// The memo holds at most `MEMO_ENTRIES` values arrays.
    #[test]
    fn memo_is_bounded() {
        let cast = expr();
        let arrays: Vec<ArrayRef> = (0..MEMO_ENTRIES + 1)
            .map(|i| Arc::new(StringArray::from(vec![format!("{i}")])) as ArrayRef)
            .collect();
        for values in &arrays {
            let input = dictionary(&[Some(0)], values);
            assert_stock(&input, &eval(&cast, &input));
        }
        assert_eq!(cast.lock().entries.len(), MEMO_ENTRIES);
        assert_eq!(cast.builds(), MEMO_ENTRIES + 1);
        // The first array was evicted and builds again.
        let input = dictionary(&[Some(0)], &arrays[0]);
        assert_stock(&input, &eval(&cast, &input));
        assert_eq!(cast.builds(), MEMO_ENTRIES + 2);
    }

    /// Inputs this cast does not memoize take the stock cast unchanged.
    #[test]
    fn other_inputs_take_the_stock_cast() {
        let plain: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let cast = expr();
        assert_stock(&plain, &eval(&cast, &plain));
        let large: ArrayRef = Arc::new(
            vec![Some("a"), None]
                .into_iter()
                .collect::<DictionaryArray<datafusion::arrow::datatypes::Int8Type>>(),
        );
        assert_stock(&large, &eval(&cast, &large));
        assert_eq!(cast.builds(), 0);
        assert!(is_memoized_input(&dict_type()));
        assert!(!is_memoized_input(large.data_type()));
    }
}
