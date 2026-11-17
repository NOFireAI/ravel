//! The seeded load (ADR-4326 decision 5): whole past hours written by
//! `ravel-cli load --ingest-hour-stride`, so a short run starts with sealed
//! hours to fold, and the records the load wrote, which the multiset check
//! expects.
//!
//! Batch `i` of the load is seeded write `SEED_WRITE_BASE + i`, holding
//! records `0..per_hour`; `ravel-cli load` writes it into ingest hour
//! `start_hour + i * stride`, and its event times fall inside that hour.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use ravel_antithesis_driver::otlp::{LOG_REC_ATTR, LOG_WRITE_ATTR};

/// Seeded write ids start here, far above any live write id.
pub const SEED_WRITE_BASE: u64 = 1 << 40;
const NS_PER_HOUR: u64 = 3_600_000_000_000;
const NS_PER_SECOND: u64 = 1_000_000_000;

/// The mapping `ravel-cli load` reads the seed file with. The two workload
/// attributes are the ones live OTLP writes carry, so one SQL query counts
/// seeded and live records alike.
pub fn mapping() -> String {
    format!(
        r#"ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "{LOG_WRITE_ATTR}"
column = "wl_write"
type = "str"

[[attribute]]
key = "{LOG_REC_ATTR}"
column = "wl_rec"
type = "str"
"#
    )
}

#[derive(Clone, Debug)]
pub struct Plan {
    pub start_hour: u32,
    pub stride: u32,
    pub hours: u32,
    pub per_hour: u32,
}

/// One seeded log record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Row {
    pub write: u64,
    pub rec: u32,
    pub ts_ns: u64,
}

impl Plan {
    /// A plan whose last batch's hour lies `stride + 3` hours before
    /// `now_ns`'s hour, so `ravel-cli load` accepts it and the fold finds every
    /// seeded hour already sealed.
    pub fn new(now_ns: u64, hours: u32, stride: u32, per_hour: u32) -> Result<Plan, String> {
        let now_hour = now_ns / NS_PER_HOUR;
        let span = u64::from(hours) * u64::from(stride);
        let start = now_hour
            .checked_sub(span + 3)
            .ok_or_else(|| "the clock is before the seeded span".to_string())?;
        Ok(Plan {
            start_hour: u32::try_from(start).map_err(|_| format!("hour {start} does not fit"))?,
            stride,
            hours,
            per_hour,
        })
    }

    /// The ingest hour of batch `i`.
    pub fn hour(&self, i: u32) -> u32 {
        self.start_hour + i * self.stride
    }

    pub fn hours_list(&self) -> Vec<u32> {
        (0..self.hours).map(|i| self.hour(i)).collect()
    }

    /// Every row of the seed file, batch by batch, in file order.
    pub fn rows(&self) -> Vec<Row> {
        let mut rows = Vec::new();
        for i in 0..self.hours {
            let hour_ns = u64::from(self.hour(i)) * NS_PER_HOUR;
            for rec in 0..self.per_hour {
                rows.push(Row {
                    write: SEED_WRITE_BASE + u64::from(i),
                    rec,
                    ts_ns: hour_ns + (u64::from(rec) + 1) * NS_PER_SECOND,
                });
            }
        }
        rows
    }

    /// The `(write, record)` identities the load stores, each exactly once.
    /// `swap` is the local loop's self-check: the expectation has one record
    /// moved to a write that does not hold it, so the multiset check must
    /// fail.
    pub fn expected(&self, swap: bool) -> BTreeSet<(u64, u32)> {
        let mut set: BTreeSet<(u64, u32)> = self.rows().iter().map(|r| (r.write, r.rec)).collect();
        if swap && self.hours >= 2 && self.per_hour >= 1 && set.remove(&(SEED_WRITE_BASE, 0)) {
            set.insert((SEED_WRITE_BASE + 1, self.per_hour));
        }
        set
    }

    /// The `ravel-cli load` arguments after the global flags.
    pub fn load_args(&self, tenant: &str, parquet: &Path, mapping: &Path) -> Vec<String> {
        vec![
            "load".into(),
            "--parquet".into(),
            parquet.display().to_string(),
            "--tenant".into(),
            tenant.into(),
            "--mapping".into(),
            mapping.display().to_string(),
            "--signal".into(),
            "logs".into(),
            "--shards".into(),
            "4".into(),
            "--batch-rows".into(),
            self.per_hour.to_string(),
            "--ingest-hour-stride".into(),
            self.stride.to_string(),
            "--ingest-hour-start".into(),
            self.start_hour.to_string(),
        ]
    }
}

/// Check `ravel-cli load`'s completion summary against the plan: the
/// `rows_written` and `rows_skipped` lines each appear exactly once, with
/// `hours * per_hour` rows written and none skipped. A load that exits 0
/// having written fewer rows would otherwise seed an expectation the store
/// never held.
pub fn check_load_report(stdout: &str, plan: &Plan) -> Result<(), String> {
    let field = |name: &str| -> Result<u64, String> {
        let values: Vec<&str> = stdout
            .lines()
            .filter_map(|l| l.trim().strip_prefix(name))
            .filter_map(|rest| rest.trim_start().strip_prefix(':'))
            .map(str::trim)
            .collect();
        match values.as_slice() {
            [v] => v
                .parse()
                .map_err(|_| format!("ravel-cli load printed {name} `{v}`, not a count")),
            _ => Err(format!(
                "ravel-cli load printed {} `{name}` lines, expected exactly one",
                values.len()
            )),
        }
    };
    let expected = u64::from(plan.hours) * u64::from(plan.per_hour);
    let written = field("rows_written")?;
    let skipped = field("rows_skipped")?;
    if written != expected || skipped != 0 {
        return Err(format!(
            "ravel-cli load wrote {written} rows and skipped {skipped}; the plan is \
             {} hours of {} rows, {expected} rows",
            plan.hours, plan.per_hour
        ));
    }
    Ok(())
}

/// Write the seed file: one row group, `per_hour` rows per batch.
pub fn write_parquet(rows: &[Row], path: &Path) -> Result<(), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("svc", DataType::Utf8, false),
        Field::new("wl_write", DataType::Utf8, false),
        Field::new("wl_rec", DataType::Utf8, false),
    ]));
    let ts: Vec<i64> = rows
        .iter()
        .map(|r| i64::try_from(r.ts_ns).map_err(|_| format!("ts {} overflows", r.ts_ns)))
        .collect::<Result<_, _>>()?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| format!("seed {} {}", r.write, r.rec)),
        )),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "seed"))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.write.to_string()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.rec.to_string()),
        )),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|e| format!("building the seed batch: {e}"))?;
    let file = std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| format!("parquet writer: {e}"))?;
    writer
        .write(&batch)
        .map_err(|e| format!("writing the seed file: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("closing the seed file: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_batch_lands_inside_its_own_past_hour() {
        let now = 500_000 * NS_PER_HOUR + 17;
        let plan = Plan::new(now, 4, 2, 3).expect("plan");
        assert_eq!(plan.start_hour, 500_000 - 11);
        // ravel-cli refuses unless start + batches * stride is below now.
        assert!(u64::from(plan.start_hour + plan.hours * plan.stride) < 500_000);
        let rows = plan.rows();
        assert_eq!(rows.len(), 12);
        for (k, r) in rows.iter().enumerate() {
            let i = u32::try_from(k).expect("fits") / plan.per_hour;
            assert_eq!(r.write, SEED_WRITE_BASE + u64::from(i));
            assert_eq!(r.ts_ns / NS_PER_HOUR, u64::from(plan.hour(i)));
        }
    }

    #[test]
    fn swapped_expectation_moves_exactly_one_record() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 3, 1, 4).expect("plan");
        let honest = plan.expected(false);
        let swapped = plan.expected(true);
        assert_eq!(honest.len(), swapped.len());
        let gone: Vec<_> = honest.difference(&swapped).collect();
        let added: Vec<_> = swapped.difference(&honest).collect();
        assert_eq!(gone, vec![&(SEED_WRITE_BASE, 0)]);
        assert_eq!(added, vec![&(SEED_WRITE_BASE + 1, 4)]);
    }

    const SUMMARY: &str = "bulk load complete\n  rows_skipped     : 0\n  \
                           rows_written     : 12\n  rows/sec         : 900\n";

    #[test]
    fn the_load_report_must_match_the_plan() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 4, 2, 3).expect("plan");
        assert_eq!(check_load_report(SUMMARY, &plan), Ok(()));
        let short = SUMMARY.replace(": 12", ": 11");
        assert!(check_load_report(&short, &plan).is_err());
        let skipped = SUMMARY.replace("rows_skipped     : 0", "rows_skipped     : 1");
        assert!(check_load_report(&skipped, &plan).is_err());
        assert!(check_load_report("bulk load complete\n", &plan).is_err());
        let twice = format!("{SUMMARY}  rows_written     : 12\n");
        assert!(check_load_report(&twice, &plan).is_err());
    }

    #[test]
    fn seed_file_round_trips_its_row_count() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 2, 1, 5).expect("plan");
        let dir = std::env::temp_dir().join(format!("seed-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("seed.parquet");
        write_parquet(&plan.rows(), &path).expect("write");
        let file = std::fs::File::open(&path).expect("open");
        let reader = parquet::file::reader::SerializedFileReader::new(file).expect("reader");
        use parquet::file::reader::FileReader as _;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 10);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
