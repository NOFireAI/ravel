//! Cost of checked integer arithmetic (issue #3847).
//!
//! Three instruments, each a ratio of medians over interleaved runs:
//!
//! - (a) arrow's checked `add`/`mul` kernels against `add_wrapping`/
//!   `mul_wrapping`, over 100M `Int64` values in 8192-row batches. Band: the
//!   checked kernel takes at most 3x the wrapping one.
//! - (b) ClickBench q30's shape, `SUM(a)` and `SUM(a + 1)` to `SUM(a + 89)`,
//!   over 10M rows, with `ravel_sql::CheckedIntegerArithmetic` enabled
//!   against disabled. The statement runs unrewritten, so every row pays all
//!   ninety checked additions; Ravel's own session folds them into one
//!   aggregate first. Band: within 5%.
//! - (c) `SELECT a * b` and `SELECT x ... WHERE x + 1 > y` over the same 10M
//!   rows, on against off. Band: within 5%.
//!
//! The SQL arms run in a DataFusion session with the rule installed ahead of
//! DataFusion's own physical rules, as `ravel_sql::build_session` installs
//! it, over an in-memory table, so the figure is the arithmetic and not the
//! scan. Each run's row count is checked against the count computed from the
//! data, and the enabled arm must fail on an overflowing statement the
//! disabled arm answers; either miss fails the figure.
//!
//! Prints one line per figure with its band, the medians, min and max of each
//! arm, the load average `uptime` reports, and PASS or MISS. Exits non-zero
//! when any figure misses its band or was not produced.
//!
//! Run: `cargo run -p ravel-bench --release --features sql-latency \
//!   --bin checked_arithmetic_bench`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::compute::kernels::numeric::{add, add_wrapping, mul, mul_wrapping};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use ravel_sql::CheckedIntegerArithmetic;

const BATCH_ROWS: usize = 8192;
const KERNEL_VALUES: usize = 100_000_000;
const SQL_ROWS: usize = 10_000_000;
/// Measured runs per arm, after one unmeasured warm-up run per arm.
const RUNS: usize = 7;
const KERNEL_MAX_RATIO: f64 = 3.0;
const SQL_MAX_DEVIATION: f64 = 0.05;
/// Operand magnitude: `2^20 * 2^20` and `2^20 + 89` both stay far inside
/// `Int64`, so no arm fails and both arms do the same work.
const OPERAND_BITS: u32 = 20;

/// A deterministic operand stream (splitmix64), so every run sees the same
/// data without a dependency on an RNG crate's API.
struct Operands(u64);

impl Operands {
    fn next(&mut self) -> i64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Signed, in `[-2^OPERAND_BITS, 2^OPERAND_BITS)`.
        (z >> (64 - OPERAND_BITS - 1)) as i64 - (1i64 << OPERAND_BITS)
    }
}

fn int64_batches(values: usize, seed: u64) -> Vec<Int64Array> {
    let mut operands = Operands(seed);
    (0..values.div_ceil(BATCH_ROWS))
        .map(|i| {
            let len = BATCH_ROWS.min(values - i * BATCH_ROWS);
            Int64Array::from_iter_values((0..len).map(|_| operands.next()))
        })
        .collect()
}

fn median(values: &[f64]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.get(sorted.len() / 2).copied()
}

/// Wall time of each measured run, per arm, and for a SQL arm the planning
/// part of it.
struct Arm {
    name: &'static str,
    millis: Vec<f64>,
    plan_millis: Vec<f64>,
}

impl Arm {
    fn new(name: &'static str) -> Self {
        Arm {
            name,
            millis: Vec::with_capacity(RUNS),
            plan_millis: Vec::with_capacity(RUNS),
        }
    }

    fn median(&self) -> Option<f64> {
        median(&self.millis)
    }

    fn summary(&self) -> String {
        let min = self.millis.iter().copied().fold(f64::INFINITY, f64::min);
        let max = self.millis.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let plan = median(&self.plan_millis)
            .map_or_else(String::new, |plan| format!(",plan_median:{plan:.2}"));
        format!(
            "{}_ms=median:{:.2},min:{:.2},max:{:.2},runs:{}{plan}",
            self.name,
            self.median().unwrap_or(f64::NAN),
            min,
            max,
            self.millis.len()
        )
    }
}

enum Band {
    AtMost(f64),
    Within(f64),
}

impl Band {
    fn holds(&self, ratio: f64) -> bool {
        match self {
            Band::AtMost(max) => ratio <= *max,
            Band::Within(deviation) => (ratio - 1.0).abs() <= *deviation,
        }
    }

    fn describe(&self) -> String {
        match self {
            Band::AtMost(max) => format!("ratio<={max}"),
            Band::Within(deviation) => {
                format!("{}<=ratio<={}", 1.0 - deviation, 1.0 + deviation)
            }
        }
    }
}

fn load_average() -> String {
    Command::new("uptime")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| {
            text.split_once("load average:")
                .map(|(_, load)| load.trim().to_string())
        })
        .unwrap_or_else(|| "unavailable".to_string())
}

/// Print one figure line and return whether it passed. `failure` is a reason
/// the figure cannot pass whatever its ratio (a wrong row count, a rule that
/// did not fire).
fn report(name: &str, band: &Band, slow: &Arm, fast: &Arm, failure: Option<String>) -> bool {
    let ratio = match (slow.median(), fast.median()) {
        (Some(s), Some(f)) if slow.millis.len() >= 5 && fast.millis.len() >= 5 && f > 0.0 => {
            Some(s / f)
        }
        _ => None,
    };
    let pass = failure.is_none() && ratio.is_some_and(|r| band.holds(r));
    println!(
        "figure={name} ratio={} band={} {} {} load_average=\"{}\"{} {}",
        ratio.map_or_else(|| "missing".to_string(), |r| format!("{r:.3}")),
        band.describe(),
        slow.summary(),
        fast.summary(),
        load_average(),
        failure.map_or_else(String::new, |f| format!(" failure=\"{f}\"")),
        if pass { "PASS" } else { "MISS" },
    );
    pass
}

type Kernel = fn(&Int64Array, &Int64Array) -> Result<ArrayRef, ArrowError>;

fn time_kernel(kernel: Kernel, lhs: &[Int64Array], rhs: &[Int64Array]) -> (f64, usize) {
    let start = Instant::now();
    let mut values = 0;
    for (l, r) in lhs.iter().zip(rhs) {
        values += kernel(l, r).expect("operands stay in range").len();
    }
    (start.elapsed().as_secs_f64() * 1e3, values)
}

/// Figure (a) for one operator: checked against wrapping, interleaved, the
/// order alternating each run.
fn kernel_figure(
    name: &str,
    checked: Kernel,
    wrapping: Kernel,
    lhs: &[Int64Array],
    rhs: &[Int64Array],
) -> bool {
    let mut checked_arm = Arm::new("checked");
    let mut wrapping_arm = Arm::new("wrapping");
    let mut failure = None;
    time_kernel(checked, lhs, rhs);
    time_kernel(wrapping, lhs, rhs);
    for run in 0..RUNS {
        let order: [(Kernel, &mut Arm); 2] = if run % 2 == 0 {
            [(checked, &mut checked_arm), (wrapping, &mut wrapping_arm)]
        } else {
            [(wrapping, &mut wrapping_arm), (checked, &mut checked_arm)]
        };
        for (kernel, arm) in order {
            let (ms, values) = time_kernel(kernel, lhs, rhs);
            if values != KERNEL_VALUES {
                failure = Some(format!("{} processed {values} values", arm.name));
            }
            arm.millis.push(ms);
        }
    }
    report(
        name,
        &Band::AtMost(KERNEL_MAX_RATIO),
        &checked_arm,
        &wrapping_arm,
        failure,
    )
}

fn session(checked: bool, table: &Arc<MemTable>) -> SessionContext {
    let partitions = std::thread::available_parallelism().map_or(1, |n| n.get());
    let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
        std::iter::once(Arc::new(CheckedIntegerArithmetic::new(checked))
            as Arc<dyn PhysicalOptimizerRule + Send + Sync>)
        .chain(PhysicalOptimizer::new().rules)
        .collect();
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(partitions))
        .with_default_features()
        .with_physical_optimizer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", Arc::clone(table) as _)
        .expect("register");
    ctx
}

/// One statement's wall time, the part of it spent planning (SQL to physical
/// plan, where the rule runs), and its row count.
struct SqlRun {
    total_ms: f64,
    plan_ms: f64,
    rows: usize,
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> DFResult<SqlRun> {
    let start = Instant::now();
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let plan_ms = start.elapsed().as_secs_f64() * 1e3;
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    Ok(SqlRun {
        total_ms: start.elapsed().as_secs_f64() * 1e3,
        plan_ms,
        rows,
    })
}

/// Figures (b) and (c): `sql` with the rule enabled against disabled,
/// interleaved, the order alternating each run.
async fn sql_figure(
    name: &str,
    sql: &str,
    expected_rows: usize,
    on: &SessionContext,
    off: &SessionContext,
) -> bool {
    let mut on_arm = Arm::new("on");
    let mut off_arm = Arm::new("off");
    let mut failure = None;
    for ctx in [on, off] {
        if let Err(err) = run_sql(ctx, sql).await {
            failure = Some(format!("warm-up failed: {err}"));
        }
    }
    for run in 0..RUNS {
        let order: [(&SessionContext, &mut Arm); 2] = if run % 2 == 0 {
            [(on, &mut on_arm), (off, &mut off_arm)]
        } else {
            [(off, &mut off_arm), (on, &mut on_arm)]
        };
        for (ctx, arm) in order {
            match run_sql(ctx, sql).await {
                Ok(run) => {
                    if run.rows != expected_rows {
                        failure = Some(format!(
                            "{} returned {} rows, want {expected_rows}",
                            arm.name, run.rows
                        ));
                    }
                    arm.millis.push(run.total_ms);
                    arm.plan_millis.push(run.plan_ms);
                }
                Err(err) => failure = Some(format!("{} failed: {err}", arm.name)),
            }
        }
    }
    report(
        name,
        &Band::Within(SQL_MAX_DEVIATION),
        &on_arm,
        &off_arm,
        failure,
    )
}

fn q30_sql() -> String {
    let terms: Vec<String> = std::iter::once("SUM(a)".to_string())
        .chain((1..90).map(|k| format!("SUM(a + {k})")))
        .collect();
    format!("SELECT {} FROM t", terms.join(", "))
}

fn sql_table() -> (Arc<MemTable>, usize) {
    let schema = Arc::new(Schema::new(
        ["a", "b", "x", "y"]
            .map(|name| Field::new(name, DataType::Int64, false))
            .to_vec(),
    ));
    let columns: Vec<Vec<Int64Array>> = (0..4u64)
        .map(|seed| int64_batches(SQL_ROWS, seed + 11))
        .collect();
    let mut filter_rows = 0;
    let batches: Vec<RecordBatch> = (0..columns[0].len())
        .map(|i| {
            let (x, y) = (&columns[2][i], &columns[3][i]);
            filter_rows += x
                .values()
                .iter()
                .zip(y.values())
                .filter(|(x, y)| **x + 1 > **y)
                .count();
            RecordBatch::try_new(
                Arc::clone(&schema),
                columns
                    .iter()
                    .map(|column| Arc::new(column[i].clone()) as ArrayRef)
                    .collect(),
            )
            .expect("batch")
        })
        .collect();
    let partitions = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut split: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
    for (i, batch) in batches.into_iter().enumerate() {
        split[i % partitions].push(batch);
    }
    let table = MemTable::try_new(schema, split).expect("table");
    (Arc::new(table), filter_rows)
}

/// The enabled arm must fail where the disabled arm wraps, or the "on"
/// figures measured a rule that never fired.
async fn rule_fires(on: &SessionContext, off: &SessionContext) -> bool {
    let sql = "SELECT a + 9223372036854775807 FROM t";
    let on_fails = run_sql(on, sql).await.is_err();
    let off_answers = run_sql(off, sql)
        .await
        .is_ok_and(|run| run.rows == SQL_ROWS);
    let pass = on_fails && off_answers;
    println!(
        "figure=rule_fires on_fails={on_fails} off_answers={off_answers} band=both_true {}",
        if pass { "PASS" } else { "MISS" }
    );
    pass
}

fn main() -> ExitCode {
    println!(
        "checked_arithmetic_bench kernel_values={KERNEL_VALUES} batch_rows={BATCH_ROWS} \
         sql_rows={SQL_ROWS} runs_per_arm={RUNS}"
    );
    let mut passed = Vec::new();

    let lhs = int64_batches(KERNEL_VALUES, 1);
    let rhs = int64_batches(KERNEL_VALUES, 2);
    passed.push(kernel_figure("kernel_add", add_kernel, add_wrapping_kernel, &lhs, &rhs));
    passed.push(kernel_figure("kernel_mul", mul_kernel, mul_wrapping_kernel, &lhs, &rhs));
    drop((lhs, rhs));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (table, filter_rows) = sql_table();
        let on = session(true, &table);
        let off = session(false, &table);
        passed.push(rule_fires(&on, &off).await);
        passed.push(sql_figure("sql_q30_shape", &q30_sql(), 1, &on, &off).await);
        passed.push(sql_figure("sql_project_mul", "SELECT a * b FROM t", SQL_ROWS, &on, &off).await);
        passed.push(
            sql_figure(
                "sql_filter_add",
                "SELECT x FROM t WHERE x + 1 > y",
                filter_rows,
                &on,
                &off,
            )
            .await,
        );
    });

    const FIGURES: usize = 6;
    let all = passed.len() == FIGURES && passed.iter().all(|p| *p);
    println!(
        "checked_arithmetic_bench figures={}/{FIGURES} {}",
        passed.iter().filter(|p| **p).count(),
        if all { "PASS" } else { "MISS" }
    );
    if all {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn add_kernel(l: &Int64Array, r: &Int64Array) -> Result<ArrayRef, ArrowError> {
    add(l, r)
}

fn add_wrapping_kernel(l: &Int64Array, r: &Int64Array) -> Result<ArrayRef, ArrowError> {
    add_wrapping(l, r)
}

fn mul_kernel(l: &Int64Array, r: &Int64Array) -> Result<ArrayRef, ArrowError> {
    mul(l, r)
}

fn mul_wrapping_kernel(l: &Int64Array, r: &Int64Array) -> Result<ArrayRef, ArrowError> {
    mul_wrapping(l, r)
}
