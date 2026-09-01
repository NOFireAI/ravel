//! Time and UTC calendar function family: `vector`,
//! `scalar`, `time`, `timestamp`, and the eight calendar functions
//! (`minute`, `hour`, `day_of_week`, `day_of_month`, `day_of_year`,
//! `days_in_month`, `month`, `year`). Calendar math is Howard Hinnant's
//! pure-integer `days_from_civil`/`civil_from_days` algorithm
//! (<https://howardhinnant.github.io/date_algorithms.html>), chosen so this
//! phase adds no calendar/date dependency to the workspace (none is present
//! today; `humantime` only formats durations, not calendar fields).
//!
//! `timestamp(v)` is the one function that reads
//! [`InstantSample::orig_sample_ts_ns`] instead of resetting it: see that
//! field's doc comment in `eval.rs` for the full rule.

use ravel_types::LabelSet;

use crate::eval::{
    Error, Evaluator, InstantSample, InstantVector, QueryWindow, Value, drop_metric_name,
};
use crate::source::SeriesSource;

use super::{FunctionDef, FunctionKind, scalar_arg, vector_arg};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "vector",
        kind: FunctionKind::Instant(vector_fn),
    },
    FunctionDef {
        name: "scalar",
        kind: FunctionKind::Instant(scalar_fn),
    },
    FunctionDef {
        name: "time",
        kind: FunctionKind::Instant(time_fn),
    },
    FunctionDef {
        name: "timestamp",
        kind: FunctionKind::VectorMap(timestamp_fn),
    },
    FunctionDef {
        name: "minute",
        kind: FunctionKind::Instant(minute),
    },
    FunctionDef {
        name: "hour",
        kind: FunctionKind::Instant(hour),
    },
    FunctionDef {
        name: "day_of_week",
        kind: FunctionKind::Instant(day_of_week),
    },
    FunctionDef {
        name: "day_of_month",
        kind: FunctionKind::Instant(day_of_month),
    },
    FunctionDef {
        name: "day_of_year",
        kind: FunctionKind::Instant(day_of_year),
    },
    FunctionDef {
        name: "days_in_month",
        kind: FunctionKind::Instant(days_in_month_fn),
    },
    FunctionDef {
        name: "month",
        kind: FunctionKind::Instant(month),
    },
    FunctionDef {
        name: "year",
        kind: FunctionKind::Instant(year),
    },
];

fn ns_to_seconds(ts_ns: i64) -> f64 {
    ts_ns as f64 / 1e9
}

fn vector_fn(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let s = scalar_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    Ok(Value::Vector(vec![InstantSample {
        labels: LabelSet::default(),
        ts_ns: eval_ts_ns,
        orig_sample_ts_ns: eval_ts_ns,
        value: s,
        histogram: None,
    }]))
}

fn scalar_fn(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let value = if v.len() == 1 { v[0].value } else { f64::NAN };
    Ok(Value::Scalar(value))
}

fn time_fn(
    _evaluator: &Evaluator,
    _source: &dyn SeriesSource,
    _call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    _ctx: &QueryWindow,
) -> Result<Value, Error> {
    Ok(Value::Scalar(ns_to_seconds(eval_ts_ns)))
}

fn timestamp_fn(v: InstantVector) -> InstantVector {
    v.into_iter()
        .map(|s| InstantSample {
            labels: drop_metric_name(s.labels),
            ts_ns: s.ts_ns,
            orig_sample_ts_ns: s.ts_ns,
            value: ns_to_seconds(s.orig_sample_ts_ns),
            histogram: None,
        })
        .collect()
}

/// Calendar fields of one UTC instant, computed once per sample and shared
/// by every calendar function below.
struct Fields {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    day_of_week: u32,
    day_of_year: u32,
    days_in_month: u32,
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month_for(year: i64, month: u32) -> u32 {
    const LENGTHS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if month == 2 && is_leap(year) {
        29
    } else {
        LENGTHS[(month - 1) as usize]
    }
}

/// Howard Hinnant's `days_from_civil`: days since 1970-01-01 for a proleptic
/// Gregorian `(year, month, day)`, valid for every representable year
/// (including negative/pre-epoch years) and `month` in `1..=12`.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if month > 2 {
        month as i64 - 3
    } else {
        month as i64 + 9
    }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + day as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Howard Hinnant's `civil_from_days`: the inverse of [`days_from_civil`].
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if month <= 2 { y + 1 } else { y }, month, day)
}

/// `time.Unix(secs, 0).UTC()`'s calendar fields. `secs` truncates toward
/// zero from the sample value before this is called (Go's `int64(f64)`
/// cast), matching Prometheus' `dateWrapper`.
fn fields_from_unix_seconds(secs: i64) -> Fields {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let hour = (secs_of_day / 3600) as u32;
    let minute = ((secs_of_day % 3600) / 60) as u32;
    let (year, month, day) = civil_from_days(days);
    // 1970-01-01 (days == 0) was a Thursday; 0 == Sunday .. 6 == Saturday,
    // matching Go's `time.Weekday`.
    let day_of_week = (days + 4).rem_euclid(7) as u32;
    let day_of_year = (days - days_from_civil(year, 1, 1) + 1) as u32;
    let days_in_month = days_in_month_for(year, month);
    Fields {
        year,
        month,
        day,
        hour,
        minute,
        day_of_week,
        day_of_year,
        days_in_month,
    }
}

/// Shared body for every calendar function: resolve the (possibly
/// defaulted-to-`vector(time())`) argument, then map each sample's value
/// (interpreted as Unix seconds) through `field`.
fn calendar_call(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
    field: impl Fn(&Fields) -> f64,
) -> Result<Value, Error> {
    let v = if call.args.args.is_empty() {
        vec![InstantSample {
            labels: LabelSet::default(),
            ts_ns: eval_ts_ns,
            orig_sample_ts_ns: eval_ts_ns,
            value: ns_to_seconds(eval_ts_ns),
            histogram: None,
        }]
    } else {
        vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?
    };
    let out = v
        .into_iter()
        .map(|s| {
            let f = fields_from_unix_seconds(s.value as i64);
            InstantSample {
                labels: drop_metric_name(s.labels),
                ts_ns: s.ts_ns,
                orig_sample_ts_ns: s.ts_ns,
                value: field(&f),
                histogram: None,
            }
        })
        .collect();
    Ok(Value::Vector(out))
}

fn minute(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| {
        f.minute as f64
    })
}

fn hour(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| f.hour as f64)
}

fn day_of_week(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| {
        f.day_of_week as f64
    })
}

fn day_of_month(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| f.day as f64)
}

fn day_of_year(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| {
        f.day_of_year as f64
    })
}

fn days_in_month_fn(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| {
        f.days_in_month as f64
    })
}

fn month(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| f.month as f64)
}

fn year(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    calendar_call(evaluator, source, call, eval_ts_ns, ctx, |f| f.year as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_round_trips() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn epoch_was_a_thursday() {
        let f = fields_from_unix_seconds(0);
        assert_eq!(f.day_of_week, 4);
        assert_eq!((f.year, f.month, f.day), (1970, 1, 1));
    }

    #[test]
    fn pre_epoch_dates_round_trip() {
        // 1969-12-31 is one day before the epoch.
        let days = days_from_civil(1969, 12, 31);
        assert_eq!(days, -1);
        assert_eq!(civil_from_days(days), (1969, 12, 31));
        let f = fields_from_unix_seconds(-1);
        assert_eq!((f.year, f.month, f.day), (1969, 12, 31));
        assert_eq!((f.hour, f.minute), (23, 59));
    }

    #[test]
    fn leap_year_february_has_29_days() {
        assert!(is_leap(2000));
        assert!(!is_leap(1900));
        assert!(is_leap(2024));
        assert_eq!(days_in_month_for(2024, 2), 29);
        assert_eq!(days_in_month_for(2023, 2), 28);
        assert_eq!(days_in_month_for(1900, 2), 28);
        assert_eq!(days_in_month_for(2000, 2), 29);
    }

    #[test]
    fn day_of_year_matches_known_date() {
        // 2024-03-01 is day 61 of a leap year (31 + 29 + 1).
        let days = days_from_civil(2024, 3, 1);
        let f = fields_from_unix_seconds(days * 86_400);
        assert_eq!(f.day_of_year, 61);
    }
}
