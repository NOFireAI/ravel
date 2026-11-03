//! One cell of an executed page, read back as a [`ResumeValue`].
//!
//! A page's cursor position is its last row's value under each effective
//! `ORDER BY` term ([`crate::PagePlan::order_by`]), and the only place a
//! paging caller can read that value from is the page's own record batch.
//! [`resume_value`] is that reading. The executed page walks in
//! `tests/page_walk.rs` resume through it, so a caller that mints cursors with
//! it pages the way those walks prove.

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::page_plan::ResumeValue;

/// The resume value of the cell at `index` of `array`, or `None` when there is
/// none to resume from.
///
/// `None` covers two cases, and a caller that needs to tell them apart checks
/// [`Array::is_null`] first:
///
/// - a NULL cell, because [`ResumeValue`] has no NULL variant (a keyset
///   comparison against NULL selects no rows);
/// - an array type the match below does not list, among them a dictionary, a
///   list, a decimal, a view type, and a timestamp of any unit but
///   nanoseconds or with a timezone. [`crate::plan_page`] refuses an order
///   term of each of those before a page is planned.
///
/// An `index` past the end of `array` is also `None`.
///
/// Every integer width widens into [`ResumeValue::Int`] or
/// [`ResumeValue::UInt`] by its signedness, and every float width into
/// [`ResumeValue::Float`] by `f64::from`, which is exact and keeps the sign of
/// a zero and the payload of a NaN. A float is returned as it is, non-finite
/// or not: [`crate::plan_page`] proves no float order term, so it refuses a
/// float in a resume tuple by type
/// ([`crate::PagePlanError::ResumeValueTypeMismatch`]), and rendering one
/// that is non-finite is refused on its own
/// ([`crate::PagePlanError::NonFiniteResumeValue`]).
pub fn resume_value(array: &dyn Array, index: usize) -> Option<ResumeValue> {
    if index >= array.len() || array.is_null(index) {
        return None;
    }
    let any = array.as_any();
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => ResumeValue::TimestampNanos(
            any.downcast_ref::<TimestampNanosecondArray>()?.value(index),
        ),
        DataType::Float16 => {
            ResumeValue::Float(f64::from(any.downcast_ref::<Float16Array>()?.value(index)))
        }
        DataType::Float32 => {
            ResumeValue::Float(f64::from(any.downcast_ref::<Float32Array>()?.value(index)))
        }
        DataType::Float64 => ResumeValue::Float(any.downcast_ref::<Float64Array>()?.value(index)),
        DataType::Int8 => {
            ResumeValue::Int(i64::from(any.downcast_ref::<Int8Array>()?.value(index)))
        }
        DataType::Int16 => {
            ResumeValue::Int(i64::from(any.downcast_ref::<Int16Array>()?.value(index)))
        }
        DataType::Int32 => {
            ResumeValue::Int(i64::from(any.downcast_ref::<Int32Array>()?.value(index)))
        }
        DataType::Int64 => ResumeValue::Int(any.downcast_ref::<Int64Array>()?.value(index)),
        DataType::UInt8 => {
            ResumeValue::UInt(u64::from(any.downcast_ref::<UInt8Array>()?.value(index)))
        }
        DataType::UInt16 => {
            ResumeValue::UInt(u64::from(any.downcast_ref::<UInt16Array>()?.value(index)))
        }
        DataType::UInt32 => {
            ResumeValue::UInt(u64::from(any.downcast_ref::<UInt32Array>()?.value(index)))
        }
        DataType::UInt64 => ResumeValue::UInt(any.downcast_ref::<UInt64Array>()?.value(index)),
        DataType::Boolean => ResumeValue::Bool(any.downcast_ref::<BooleanArray>()?.value(index)),
        DataType::Utf8 => {
            ResumeValue::Str(any.downcast_ref::<StringArray>()?.value(index).to_string())
        }
        DataType::LargeUtf8 => ResumeValue::Str(
            any.downcast_ref::<LargeStringArray>()?
                .value(index)
                .to_string(),
        ),
        DataType::FixedSizeBinary(_) => ResumeValue::FixedSizeBinary(
            any.downcast_ref::<FixedSizeBinaryArray>()?
                .value(index)
                .to_vec(),
        ),
        DataType::Binary => {
            ResumeValue::Binary(any.downcast_ref::<BinaryArray>()?.value(index).to_vec())
        }
        DataType::LargeBinary => ResumeValue::Binary(
            any.downcast_ref::<LargeBinaryArray>()?
                .value(index)
                .to_vec(),
        ),
        _ => return None,
    };
    Some(value)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Decimal128Array, DictionaryArray, ListArray, StringViewArray,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::Int32Type;

    use super::*;
    use crate::page_plan::{CursorSupport, cursor_support};

    /// The resume value of the array's only cell, after checking that the
    /// planner admits an order term of the array's type and that the kind it
    /// proves for that term admits the value read back, so every case here
    /// is also a case of the two agreeing and a value minted from a page's
    /// last row passes the planner's resume type check.
    fn exact(array: ArrayRef) -> ResumeValue {
        let CursorSupport::Exact(kind) = cursor_support(array.data_type()) else {
            panic!("plan_page admits an order term of type {}", array.data_type());
        };
        let value = resume_value(array.as_ref(), 0).expect("a non-NULL cell of an admitted type");
        assert!(
            kind.admits(&value),
            "the kind proven for {} admits {value:?}",
            array.data_type(),
        );
        value
    }

    /// The resume value of the array's only cell, for a float type, which
    /// the planner refuses as an order term but the converter still reads.
    fn float(array: ArrayRef) -> f64 {
        match resume_value(array.as_ref(), 0) {
            Some(ResumeValue::Float(value)) => value,
            other => panic!("{} reads as {other:?}", array.data_type()),
        }
    }

    #[test]
    fn every_integer_width_widens_by_its_signedness() {
        assert_eq!(
            exact(Arc::new(Int8Array::from(vec![i8::MIN]))),
            ResumeValue::Int(-128),
        );
        assert_eq!(
            exact(Arc::new(Int16Array::from(vec![i16::MIN]))),
            ResumeValue::Int(-32_768),
        );
        assert_eq!(
            exact(Arc::new(Int32Array::from(vec![i32::MIN]))),
            ResumeValue::Int(-2_147_483_648),
        );
        assert_eq!(
            exact(Arc::new(Int64Array::from(vec![i64::MIN]))),
            ResumeValue::Int(i64::MIN),
        );
        assert_eq!(
            exact(Arc::new(UInt8Array::from(vec![u8::MAX]))),
            ResumeValue::UInt(255),
        );
        assert_eq!(
            exact(Arc::new(UInt16Array::from(vec![u16::MAX]))),
            ResumeValue::UInt(65_535),
        );
        assert_eq!(
            exact(Arc::new(UInt32Array::from(vec![u32::MAX]))),
            ResumeValue::UInt(4_294_967_295),
        );
        assert_eq!(
            exact(Arc::new(UInt64Array::from(vec![u64::MAX]))),
            ResumeValue::UInt(u64::MAX),
        );
    }

    #[test]
    fn bool_string_binary_and_timestamp_cells_read_back_unchanged() {
        assert_eq!(
            exact(Arc::new(BooleanArray::from(vec![true]))),
            ResumeValue::Bool(true),
        );
        assert_eq!(
            exact(Arc::new(BooleanArray::from(vec![false]))),
            ResumeValue::Bool(false),
        );
        assert_eq!(
            exact(Arc::new(StringArray::from(vec!["it's é"]))),
            ResumeValue::Str("it's é".to_string()),
        );
        assert_eq!(
            exact(Arc::new(LargeStringArray::from(vec!["large"]))),
            ResumeValue::Str("large".to_string()),
        );
        assert_eq!(
            exact(Arc::new(BinaryArray::from(vec![&[0u8, 0xff][..]]))),
            ResumeValue::Binary(vec![0, 0xff]),
        );
        assert_eq!(
            exact(Arc::new(LargeBinaryArray::from(vec![&[7u8][..]]))),
            ResumeValue::Binary(vec![7]),
        );
        assert_eq!(
            exact(Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2, 3]].into_iter())
                    .expect("fixed size binary"),
            )),
            ResumeValue::FixedSizeBinary(vec![1, 2, 3]),
        );
        assert_eq!(
            exact(Arc::new(TimestampNanosecondArray::from(vec![
                -1_234_567_891i64
            ]))),
            ResumeValue::TimestampNanos(-1_234_567_891),
        );
    }

    /// Every float width reads back bit for bit: the sign of a zero, a value
    /// with no short decimal spelling, and a NaN payload.
    ///
    /// FLIP: with the `Float32` arm reading `value.to_string().parse::<f64>()`
    /// (the cell through decimal text), `0.1f32` reads back as the `f64`
    /// nearest 0.1 rather than as the `f32` nearest it, and the `0.1` row
    /// fails. With it reading `f64::from(value) + 0.0`, the `-0.0` row reads
    /// back as `0.0` and fails.
    #[test]
    fn every_float_width_reads_back_exactly() {
        let f32s: ArrayRef = Arc::new(Float32Array::from(vec![
            -0.0f32,
            0.0,
            0.1,
            f32::from_bits(0x7fc0_0001),
        ]));
        for (index, expected) in [-0.0f32, 0.0, 0.1, f32::from_bits(0x7fc0_0001)]
            .into_iter()
            .enumerate()
        {
            let Some(ResumeValue::Float(read)) = resume_value(f32s.as_ref(), index) else {
                panic!("Float32 row {index} reads as no float");
            };
            assert_eq!(
                read.to_bits(),
                f64::from(expected).to_bits(),
                "Float32 row {index} ({expected:?}) reads back as {read:?}",
            );
        }

        for value in [-0.0f64, 0.0, 0.1, f64::from_bits(0x7ff8_0000_0000_0001)] {
            let read = float(Arc::new(Float64Array::from(vec![value])));
            assert_eq!(
                read.to_bits(),
                value.to_bits(),
                "Float64 {value:?} reads back as {read:?}",
            );
        }

        for value in [-0.0f32, 0.5] {
            let halves = cast(
                &(Arc::new(Float32Array::from(vec![value])) as ArrayRef),
                &DataType::Float16,
            )
            .expect("cast to Float16");
            let read = float(halves);
            assert_eq!(
                read.to_bits(),
                f64::from(value).to_bits(),
                "Float16 {value:?} reads back as {read:?}",
            );
        }
    }

    /// A NULL cell has no resume value, whatever the type, and a non-NULL
    /// cell beside it in the same array still does.
    #[test]
    fn a_null_cell_reads_as_none() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(5)]));
        assert_eq!(resume_value(ints.as_ref(), 0), None);
        assert_eq!(resume_value(ints.as_ref(), 1), Some(ResumeValue::Int(5)));

        let strings: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        assert_eq!(resume_value(strings.as_ref(), 0), None);

        let floats: ArrayRef = Arc::new(Float64Array::from(vec![None::<f64>]));
        assert_eq!(resume_value(floats.as_ref(), 0), None);
    }

    #[test]
    fn an_index_past_the_end_reads_as_none() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        assert_eq!(resume_value(ints.as_ref(), 1), None);
    }

    /// Every type the converter has no variant for reads as `None` rather than
    /// panicking, and the planner refuses an order term of each of them.
    #[test]
    fn a_type_no_variant_carries_reads_as_none() {
        let dictionary: DictionaryArray<Int32Type> = vec!["a"].into_iter().collect();
        let unrepresentable: Vec<ArrayRef> = vec![
            Arc::new(dictionary),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1)]),
            ])),
            Arc::new(StringViewArray::from(vec!["view"])),
            Arc::new(
                Decimal128Array::from(vec![1_i128])
                    .with_precision_and_scale(10, 2)
                    .expect("decimal"),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![1_i64])),
            Arc::new(TimestampNanosecondArray::from(vec![1_i64]).with_timezone("UTC")),
        ];
        for array in unrepresentable {
            assert_eq!(
                resume_value(array.as_ref(), 0),
                None,
                "type {}",
                array.data_type(),
            );
            assert!(
                matches!(
                    cursor_support(array.data_type()),
                    CursorSupport::Unrepresentable(_)
                ),
                "plan_page refuses an order term of type {}",
                array.data_type(),
            );
        }
    }
}
