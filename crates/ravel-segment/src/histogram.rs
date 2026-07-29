//! Native-histogram value model and HIST_SPANS record encoding for RSEG v3
//! (ADR-0017, docs/rseg-v3-plan.md section 2/3.5). Span-based, the superset
//! of OTLP's contiguous shape and Prometheus's sparse shape.

use crate::error::WriteError;
use crate::varint::{write_uvarint, write_zigzag_varint};

/// Reset-hint states carried per histogram sample (Prometheus's four
/// states; OTLP's `AggregationTemporality` maps onto them upstream of this
/// crate, not here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

impl ResetHint {
    fn wire(self) -> u8 {
        match self {
            ResetHint::Unknown => 0,
            ResetHint::Yes => 1,
            ResetHint::No => 2,
            ResetHint::Gauge => 3,
        }
    }
}

/// One populated run on a histogram side. `offset` is relative to the end
/// of the previous span on that side (relative to index 0 for the first
/// span); `length` is the run's bucket count and MUST be `> 0`
/// (docs/rseg-v3-plan.md section 3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistogramSpan {
    pub offset: i32,
    pub length: u32,
}

/// Bucket/zero/total counts for one histogram sample: all-integer or
/// all-float, never mixed within one sample (docs/rseg-v3-plan.md
/// section 2). `positive`/`negative` must have exactly
/// `sum(span.length)` entries for their respective side.
#[derive(Debug, Clone, PartialEq)]
pub enum HistogramCounts {
    Int {
        zero_count: u64,
        count: u64,
        positive: Vec<u64>,
        negative: Vec<u64>,
    },
    Float {
        zero_count: f64,
        count: f64,
        positive: Vec<f64>,
        negative: Vec<f64>,
    },
}

/// One native-histogram sample value (docs/rseg-v3-plan.md section 2/3.5).
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramValue {
    pub scale: i32,
    pub zero_threshold: f64,
    pub sum: Option<f64>,
    /// Present iff `scale == -53`; ascending boundaries.
    pub custom_values: Option<Vec<f64>>,
    pub positive_spans: Vec<HistogramSpan>,
    pub negative_spans: Vec<HistogramSpan>,
    pub counts: HistogramCounts,
    pub reset_hint: ResetHint,
}

fn spans_side_len(spans: &[HistogramSpan]) -> Result<u64, WriteError> {
    let mut total: u64 = 0;
    for span in spans {
        if span.length == 0 {
            return Err(WriteError::HistogramSpanLengthZero);
        }
        total = total
            .checked_add(u64::from(span.length))
            .ok_or(WriteError::HistogramBucketCountLenMismatch)?;
    }
    Ok(total)
}

fn write_side_spans(out: &mut Vec<u8>, spans: &[HistogramSpan]) -> Result<(), WriteError> {
    write_uvarint(out, spans.len() as u64);
    for span in spans {
        write_zigzag_varint(out, i64::from(span.offset));
        write_uvarint(out, u64::from(span.length));
    }
    Ok(())
}

/// Validates `value` against the writer-side structural invariants
/// (docs/rseg-v3-plan.md section 3.5) and appends its HIST_SPANS record
/// encoding to `out`. Also enforces `count >= zero_count` and `count >=
/// sum(bucket_counts)`, the same consistency rule the reader checks at
/// decode time (`SegmentError::HistogramCountInconsistent`), so an
/// inconsistent value is rejected here rather than becoming permanently
/// undecodable data once committed to object storage.
pub(crate) fn encode_histogram_record_into(
    out: &mut Vec<u8>,
    value: &HistogramValue,
) -> Result<(), WriteError> {
    if value.scale < -53 {
        return Err(WriteError::HistogramScaleTooSmall(value.scale));
    }
    let is_custom_scale = value.scale == -53;
    match &value.custom_values {
        Some(bounds) => {
            if !is_custom_scale || bounds.is_empty() {
                return Err(WriteError::HistogramCustomValuesMismatch);
            }
            if !bounds.windows(2).all(|w| w[0] < w[1]) {
                return Err(WriteError::HistogramCustomValuesMismatch);
            }
        }
        None => {
            if is_custom_scale {
                return Err(WriteError::HistogramCustomValuesMismatch);
            }
        }
    }

    let positive_expected = spans_side_len(&value.positive_spans)?;
    let negative_expected = spans_side_len(&value.negative_spans)?;

    let count_kind: u8 = match &value.counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => {
            if positive.len() as u64 != positive_expected
                || negative.len() as u64 != negative_expected
            {
                return Err(WriteError::HistogramBucketCountLenMismatch);
            }
            let total = positive
                .iter()
                .chain(negative.iter())
                .try_fold(0u64, |acc, &v| acc.checked_add(v))
                .ok_or(WriteError::HistogramCountInconsistent)?;
            if *count < *zero_count || *count < total {
                return Err(WriteError::HistogramCountInconsistent);
            }
            0
        }
        HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => {
            if positive.len() as u64 != positive_expected
                || negative.len() as u64 != negative_expected
            {
                return Err(WriteError::HistogramBucketCountLenMismatch);
            }
            let total: f64 = positive.iter().chain(negative.iter()).sum();
            if *count < *zero_count || *count < total {
                return Err(WriteError::HistogramCountInconsistent);
            }
            1
        }
    };

    let has_sum = value.sum.is_some();
    let flags = count_kind | (u8::from(has_sum) << 1) | (value.reset_hint.wire() << 2);
    out.push(flags);

    write_zigzag_varint(out, i64::from(value.scale));
    out.extend_from_slice(&value.zero_threshold.to_le_bytes());

    match &value.counts {
        HistogramCounts::Int {
            zero_count, count, ..
        } => {
            write_uvarint(out, *zero_count);
            write_uvarint(out, *count);
        }
        HistogramCounts::Float {
            zero_count, count, ..
        } => {
            out.extend_from_slice(&zero_count.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
        }
    }

    if let Some(sum) = value.sum {
        out.extend_from_slice(&sum.to_le_bytes());
    }

    if let Some(bounds) = &value.custom_values {
        write_uvarint(out, bounds.len() as u64);
        for b in bounds {
            out.extend_from_slice(&b.to_le_bytes());
        }
    }

    write_side_spans(out, &value.positive_spans)?;
    match &value.counts {
        HistogramCounts::Int { positive, .. } => {
            for v in positive {
                write_uvarint(out, *v);
            }
        }
        HistogramCounts::Float { positive, .. } => {
            for v in positive {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    write_side_spans(out, &value.negative_spans)?;
    match &value.counts {
        HistogramCounts::Int { negative, .. } => {
            for v in negative {
                write_uvarint(out, *v);
            }
        }
        HistogramCounts::Float { negative, .. } => {
            for v in negative {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_value(counts: HistogramCounts) -> HistogramValue {
        HistogramValue {
            scale: 3,
            zero_threshold: 0.001,
            sum: Some(1.5),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            counts,
            reset_hint: ResetHint::Unknown,
        }
    }

    #[test]
    fn int_count_below_zero_count_is_rejected() {
        let value = base_value(HistogramCounts::Int {
            zero_count: 10,
            count: 3,
            positive: vec![1, 2],
            negative: vec![],
        });
        let mut out = Vec::new();
        assert_eq!(
            encode_histogram_record_into(&mut out, &value),
            Err(WriteError::HistogramCountInconsistent)
        );
    }

    #[test]
    fn int_count_below_bucket_sum_is_rejected() {
        let value = base_value(HistogramCounts::Int {
            zero_count: 0,
            count: 2,
            positive: vec![1, 2],
            negative: vec![],
        });
        let mut out = Vec::new();
        assert_eq!(
            encode_histogram_record_into(&mut out, &value),
            Err(WriteError::HistogramCountInconsistent)
        );
    }

    #[test]
    fn float_count_below_zero_count_is_rejected() {
        let value = base_value(HistogramCounts::Float {
            zero_count: 10.0,
            count: 3.0,
            positive: vec![1.0, 2.0],
            negative: vec![],
        });
        let mut out = Vec::new();
        assert_eq!(
            encode_histogram_record_into(&mut out, &value),
            Err(WriteError::HistogramCountInconsistent)
        );
    }

    #[test]
    fn float_count_below_bucket_sum_is_rejected() {
        let value = base_value(HistogramCounts::Float {
            zero_count: 0.0,
            count: 2.0,
            positive: vec![1.0, 2.0],
            negative: vec![],
        });
        let mut out = Vec::new();
        assert_eq!(
            encode_histogram_record_into(&mut out, &value),
            Err(WriteError::HistogramCountInconsistent)
        );
    }

    #[test]
    fn consistent_counts_are_accepted() {
        let int_value = base_value(HistogramCounts::Int {
            zero_count: 1,
            count: 3,
            positive: vec![1, 2],
            negative: vec![],
        });
        let mut out = Vec::new();
        assert!(encode_histogram_record_into(&mut out, &int_value).is_ok());

        let float_value = base_value(HistogramCounts::Float {
            zero_count: 1.0,
            count: 3.0,
            positive: vec![1.0, 2.0],
            negative: vec![],
        });
        let mut out = Vec::new();
        assert!(encode_histogram_record_into(&mut out, &float_value).is_ok());
    }
}
