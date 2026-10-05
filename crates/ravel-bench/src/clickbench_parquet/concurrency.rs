//! The ClickBench Parquet lane's concurrency phase (ADR-2040 D7): N tasks,
//! each with its own engine, cycle the suite's statements until the phase's
//! duration has passed, and the figures D7 reports (queries per second,
//! error ratio, per-statement counts) come out of what they completed.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use super::engine::SuiteEngine;
use super::suite::Statement;

/// Task `i` starts at statement `i * TASK_OFFSET_STRIDE` (mod the statement
/// count), so ten tasks start spread across the 43 statements instead of
/// all hitting Q1 at once.
pub const TASK_OFFSET_STRIDE: usize = 4;

/// D7's ten connections.
pub const DEFAULT_TASKS: usize = 10;

/// D7's 600 s phase.
pub const DEFAULT_DURATION: Duration = Duration::from_secs(600);

/// Time since an arbitrary origin. The phase reads time only through this,
/// so a test drives it with a scripted clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
}

/// [`Clock`] over the monotonic clock, for a real run.
pub struct MonotonicClock {
    origin: std::time::Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        MonotonicClock {
            origin: std::time::Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// One concurrency task: the engine it queries through and the clock it
/// reads. A real run gives every task the same clock.
pub struct PhaseTask {
    pub engine: Arc<dyn SuiteEngine>,
    pub clock: Arc<dyn Clock>,
}

/// One statement's figures over the phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcurrencyStatement {
    pub number: u32,
    pub completed: u64,
    pub errors: u64,
    /// Nearest-rank percentiles over completed runs; `None` when none
    /// completed.
    pub p50_s: Option<f64>,
    pub p95_s: Option<f64>,
    /// The error text from the lowest-numbered task that saw this statement
    /// fail.
    pub first_error: Option<String>,
}

/// What the phase measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcurrencyFigures {
    pub tasks: usize,
    /// The configured duration: no task starts a statement past it.
    pub duration_s: f64,
    /// The measured window, on the tasks' clocks: from the earliest task
    /// start to the last statement returning, answered or errored. A
    /// statement still running at the deadline finishes, so this is at
    /// least `duration_s`.
    pub elapsed_s: f64,
    pub queries_completed: u64,
    pub errors: u64,
    /// `queries_completed / elapsed_s`. A statement still running at the
    /// deadline finishes and counts, and the window runs until it returns.
    pub qps: f64,
    /// `errors / (queries_completed + errors)`, 0 when nothing ran.
    pub error_ratio: f64,
    /// One entry per suite statement, in suite order.
    pub statements: Vec<ConcurrencyStatement>,
    /// Statements that errored at least once.
    pub errored_statements: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("the concurrency phase needs at least one task")]
    NoTasks,
    #[error("the concurrency phase needs at least one statement")]
    NoStatements,
    #[error("the concurrency phase needs a duration above zero")]
    ZeroDuration,
    #[error("a concurrency task stopped abnormally: {0}")]
    TaskFailed(String),
}

struct Outcome {
    index: usize,
    result: Result<Duration, String>,
}

/// One task's outcomes and when, on its clock, it began and its last
/// statement returned.
struct TaskRun {
    outcomes: Vec<Outcome>,
    began: Duration,
    ended: Duration,
}

async fn run_task(
    task: PhaseTask,
    task_index: usize,
    statements: Arc<Vec<Statement>>,
    duration: Duration,
) -> TaskRun {
    let len = statements.len();
    let offset = (task_index * TASK_OFFSET_STRIDE) % len;
    let began = task.clock.now();
    let deadline = began + duration;
    // Each return's reading is the next statement's start, so the loop ends
    // only once `ended` reaches the deadline.
    let mut ended = began;
    let mut outcomes = Vec::new();
    for k in 0.. {
        let started = ended;
        if started >= deadline {
            break;
        }
        let index = (offset + k) % len;
        let result = task.engine.query(&statements[index].sql).await;
        ended = task.clock.now();
        let latency = ended.saturating_sub(started);
        outcomes.push(Outcome {
            index,
            result: result.map(|_| latency).map_err(|e| e.to_string()),
        });
    }
    TaskRun {
        outcomes,
        began,
        ended,
    }
}

/// Nearest-rank percentile: the value at rank `ceil(p * n)` of the sorted
/// sample.
fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (p * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.clamp(1, sorted.len()) - 1).copied()
}

/// Every refusal [`run`] makes before it starts a task, so a caller can
/// refuse the same arguments before it runs anything else.
pub fn check_shape(
    tasks: usize,
    statements: usize,
    duration: Duration,
) -> Result<(), ConcurrencyError> {
    if tasks == 0 {
        return Err(ConcurrencyError::NoTasks);
    }
    if statements == 0 {
        return Err(ConcurrencyError::NoStatements);
    }
    if duration.is_zero() {
        return Err(ConcurrencyError::ZeroDuration);
    }
    Ok(())
}

/// Runs the phase: task `i` cycles `statements` from position
/// `i * TASK_OFFSET_STRIDE` (mod their count) and starts no statement once
/// `duration` has passed on its clock since it began. A statement already
/// running at that point runs to completion and counts.
pub async fn run(
    tasks: Vec<PhaseTask>,
    statements: &[Statement],
    duration: Duration,
) -> Result<ConcurrencyFigures, ConcurrencyError> {
    check_shape(tasks.len(), statements.len(), duration)?;
    let task_count = tasks.len();
    let shared = Arc::new(statements.to_vec());
    let mut set = JoinSet::new();
    for (task_index, task) in tasks.into_iter().enumerate() {
        let shared = Arc::clone(&shared);
        set.spawn(async move {
            (
                task_index,
                run_task(task, task_index, shared, duration).await,
            )
        });
    }
    let mut per_task: Vec<(usize, TaskRun)> = Vec::with_capacity(task_count);
    while let Some(joined) = set.join_next().await {
        per_task.push(joined.map_err(|e| ConcurrencyError::TaskFailed(e.to_string()))?);
    }
    per_task.sort_by_key(|(task_index, _)| *task_index);
    // Every task's `ended` is at least its `began` plus `duration`, so the
    // window is at least `duration`, which check_shape keeps above zero.
    let phase_start = per_task.iter().map(|(_, r)| r.began).min();
    let last_return = per_task.iter().map(|(_, r)| r.ended).max();
    let elapsed = match (phase_start, last_return) {
        (Some(start), Some(end)) => end.saturating_sub(start),
        _ => duration,
    };

    let mut latencies: Vec<Vec<f64>> = vec![Vec::new(); statements.len()];
    let mut errors = vec![0u64; statements.len()];
    let mut first_errors: Vec<Option<String>> = vec![None; statements.len()];
    for (_, task_run) in per_task {
        for outcome in task_run.outcomes {
            match outcome.result {
                Ok(latency) => latencies[outcome.index].push(latency.as_secs_f64()),
                Err(error) => {
                    errors[outcome.index] += 1;
                    first_errors[outcome.index].get_or_insert(error);
                }
            }
        }
    }

    let mut figures = Vec::with_capacity(statements.len());
    for (index, statement) in statements.iter().enumerate() {
        let sample = &mut latencies[index];
        sample.sort_by(f64::total_cmp);
        figures.push(ConcurrencyStatement {
            number: statement.number,
            completed: sample.len() as u64,
            errors: errors[index],
            p50_s: percentile(sample, 0.50),
            p95_s: percentile(sample, 0.95),
            first_error: first_errors[index].take(),
        });
    }
    let queries_completed: u64 = figures.iter().map(|s| s.completed).sum();
    let error_total: u64 = figures.iter().map(|s| s.errors).sum();
    let attempted = queries_completed + error_total;
    Ok(ConcurrencyFigures {
        tasks: task_count,
        duration_s: duration.as_secs_f64(),
        elapsed_s: elapsed.as_secs_f64(),
        queries_completed,
        errors: error_total,
        qps: queries_completed as f64 / elapsed.as_secs_f64(),
        error_ratio: if attempted == 0 {
            0.0
        } else {
            error_total as f64 / attempted as f64
        },
        errored_statements: figures
            .iter()
            .filter(|s| s.errors > 0)
            .map(|s| s.number)
            .collect(),
        statements: figures,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use async_trait::async_trait;
    use datafusion::arrow::record_batch::RecordBatch;

    use super::*;
    use crate::clickbench_parquet::engine::{DdlReceipt, EngineError};

    /// A clock that moves only when a stub engine advances it.
    #[derive(Default)]
    struct ScriptedClock {
        nanos: AtomicU64,
    }

    impl ScriptedClock {
        fn advance(&self, by: Duration) {
            self.nanos.fetch_add(by.as_nanos() as u64, Ordering::SeqCst);
        }
    }

    impl Clock for ScriptedClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
        }
    }

    /// Per statement: its latency in seconds and whether it errors.
    type Script = BTreeMap<String, (u64, bool)>;

    /// Never answers more than this many calls, so a driver that ignores
    /// the deadline fails the test instead of hanging it.
    const CALL_CAP: usize = 1000;

    struct StubEngine {
        clock: Arc<ScriptedClock>,
        script: Script,
        /// (statement SQL, clock seconds at start) per call.
        calls: Mutex<Vec<(String, u64)>>,
    }

    #[async_trait]
    impl SuiteEngine for StubEngine {
        async fn ddl(&self, _sql: &str) -> Result<DdlReceipt, EngineError> {
            Err(EngineError::Ddl("stub runs no DDL".to_string()))
        }

        async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, EngineError> {
            let (latency, errors) = *self.script.get(sql).expect("scripted statement");
            {
                let mut calls = self.calls.lock().expect("calls lock");
                assert!(calls.len() < CALL_CAP, "driver kept starting statements");
                calls.push((sql.to_string(), self.clock.now().as_secs()));
            }
            self.clock.advance(Duration::from_secs(latency));
            tokio::task::yield_now().await;
            if errors {
                Err(EngineError::Query(format!("{sql} exhausted its budget")))
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn statements(count: u32) -> Vec<Statement> {
        (1..=count)
            .map(|number| Statement {
                number,
                sql: format!("s{number}"),
            })
            .collect()
    }

    fn stub(script: &[(&str, u64, bool)]) -> Arc<StubEngine> {
        Arc::new(StubEngine {
            clock: Arc::new(ScriptedClock::default()),
            script: script
                .iter()
                .map(|(sql, latency, errors)| ((*sql).to_string(), (*latency, *errors)))
                .collect(),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn task(engine: &Arc<StubEngine>) -> PhaseTask {
        PhaseTask {
            engine: Arc::clone(engine) as Arc<dyn SuiteEngine>,
            clock: Arc::clone(&engine.clock) as Arc<dyn Clock>,
        }
    }

    fn calls(engine: &StubEngine) -> Vec<(String, u64)> {
        engine.calls.lock().expect("calls lock").clone()
    }

    #[tokio::test]
    async fn figures_match_the_hand_computation() {
        // Task 0 starts at s1, task 1 at s5 (offset 4 of 5). Over 6 s:
        //   task 0: s1 0-1, s2 1-3, s3 errors 3-6, then the deadline.
        //   task 1: s5 0-2, s1 2-5, s2 5-7 (started before 6 s, so it counts).
        // The last return is task 1's s2 at 7 s, so the window is 7 s.
        let rest = [
            ("s2", 2, false),
            ("s3", 3, true),
            ("s4", 1, false),
            ("s5", 2, false),
        ];
        let engine0 = stub(&[[("s1", 1, false)].as_slice(), &rest].concat());
        let engine1 = stub(&[[("s1", 3, false)].as_slice(), &rest].concat());
        let figures = run(
            vec![task(&engine0), task(&engine1)],
            &statements(5),
            Duration::from_secs(6),
        )
        .await
        .expect("phase runs");

        assert_eq!(figures.tasks, 2);
        assert_eq!(figures.duration_s.to_bits(), 6.0f64.to_bits());
        assert_eq!(figures.elapsed_s.to_bits(), 7.0f64.to_bits());
        assert_eq!(figures.queries_completed, 5);
        assert_eq!(figures.errors, 1);
        assert_eq!(figures.qps.to_bits(), (5.0f64 / 7.0).to_bits());
        assert_eq!(figures.error_ratio.to_bits(), (1.0f64 / 6.0).to_bits());
        let by_number: BTreeMap<u32, &ConcurrencyStatement> =
            figures.statements.iter().map(|s| (s.number, s)).collect();
        let row = |n: u32| {
            let s = by_number[&n];
            (s.completed, s.errors, s.p50_s, s.p95_s)
        };
        assert_eq!(row(1), (2, 0, Some(1.0), Some(3.0)));
        assert_eq!(row(2), (2, 0, Some(2.0), Some(2.0)));
        assert_eq!(row(3), (0, 1, None, None));
        assert_eq!(row(4), (0, 0, None, None));
        assert_eq!(row(5), (1, 0, Some(2.0), Some(2.0)));
        assert_eq!(figures.errored_statements, vec![3]);
    }

    #[tokio::test]
    async fn no_statement_starts_after_the_deadline() {
        let engine = stub(&[("s1", 4, false)]);
        let figures = run(
            vec![task(&engine)],
            &statements(1),
            Duration::from_secs(10),
        )
        .await
        .expect("phase runs");
        let starts: Vec<u64> = calls(&engine).into_iter().map(|(_, at)| at).collect();
        // The run started at 8 s finishes at 12 s and counts; none starts at 12 s.
        assert_eq!(starts, vec![0, 4, 8]);
        assert_eq!(figures.queries_completed, 3);
        // qps is over the 12 s the three runs took, not the 10 s configured.
        assert_eq!(figures.duration_s.to_bits(), 10.0f64.to_bits());
        assert_eq!(figures.elapsed_s.to_bits(), 12.0f64.to_bits());
        assert_eq!(figures.qps.to_bits(), 0.25f64.to_bits());
    }

    #[tokio::test]
    async fn each_task_starts_at_its_offset() {
        let script: Vec<(String, u64, bool)> =
            (1..=43).map(|n| (format!("s{n}"), 1, false)).collect();
        let script: Vec<(&str, u64, bool)> = script
            .iter()
            .map(|(s, l, e)| (s.as_str(), *l, *e))
            .collect();
        let engines: Vec<Arc<StubEngine>> = (0..DEFAULT_TASKS).map(|_| stub(&script)).collect();
        run(
            engines.iter().map(task).collect(),
            &statements(43),
            Duration::from_secs(2),
        )
        .await
        .expect("phase runs");
        for (i, engine) in engines.iter().enumerate() {
            let first = (i * 4) % 43 + 1;
            let second = (i * 4 + 1) % 43 + 1;
            assert_eq!(
                calls(engine),
                vec![(format!("s{first}"), 0), (format!("s{second}"), 1)],
                "task {i}"
            );
        }
    }

    #[tokio::test]
    async fn an_erroring_statement_is_counted_and_named() {
        let engine = stub(&[("s1", 1, false), ("s2", 1, true)]);
        let figures = run(
            vec![task(&engine)],
            &statements(2),
            Duration::from_secs(4),
        )
        .await
        .expect("phase runs");
        assert_eq!(figures.queries_completed, 2);
        assert_eq!(figures.errors, 2);
        assert_eq!(figures.error_ratio, 0.5);
        assert_eq!(figures.errored_statements, vec![2]);
        assert_eq!(
            figures.statements[1].first_error.as_deref(),
            Some("query failed: s2 exhausted its budget")
        );
        assert_eq!(figures.statements[0].first_error, None);
    }

    #[tokio::test]
    async fn every_statement_counts_in_the_error_ratio() {
        // One task over 6 s runs s1, s2, s3 twice each: s1 completes twice,
        // s2 and s3 error twice each.
        let engine = stub(&[("s1", 1, false), ("s2", 1, true), ("s3", 1, true)]);
        let figures = run(vec![task(&engine)], &statements(3), Duration::from_secs(6))
            .await
            .expect("phase runs");
        assert_eq!(figures.queries_completed, 2);
        assert_eq!(figures.errors, 4);
        assert_eq!(figures.error_ratio.to_bits(), (4.0f64 / 6.0).to_bits());
        assert_eq!(figures.errored_statements, vec![2, 3]);
        let per_statement: Vec<(u32, u64, u64)> = figures
            .statements
            .iter()
            .map(|s| (s.number, s.completed, s.errors))
            .collect();
        assert_eq!(per_statement, vec![(1, 2, 0), (2, 0, 2), (3, 0, 2)]);
    }

    #[tokio::test]
    async fn an_empty_phase_is_refused() {
        let engine = stub(&[("s1", 1, false)]);
        assert_eq!(
            run(Vec::new(), &statements(1), Duration::from_secs(1)).await,
            Err(ConcurrencyError::NoTasks)
        );
        assert_eq!(
            run(vec![task(&engine)], &[], Duration::from_secs(1)).await,
            Err(ConcurrencyError::NoStatements)
        );
        assert_eq!(
            run(vec![task(&engine)], &statements(1), Duration::ZERO).await,
            Err(ConcurrencyError::ZeroDuration)
        );
    }

    #[test]
    fn check_shape_refuses_what_run_refuses() {
        let second = Duration::from_secs(1);
        assert_eq!(check_shape(0, 1, second), Err(ConcurrencyError::NoTasks));
        assert_eq!(
            check_shape(1, 0, second),
            Err(ConcurrencyError::NoStatements)
        );
        assert_eq!(
            check_shape(1, 1, Duration::ZERO),
            Err(ConcurrencyError::ZeroDuration)
        );
        assert_eq!(check_shape(1, 1, second), Ok(()));
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let sample: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(percentile(&sample, 0.50), Some(10.0));
        assert_eq!(percentile(&sample, 0.95), Some(19.0));
        assert_eq!(percentile(&[7.0], 0.95), Some(7.0));
        assert_eq!(percentile(&[], 0.5), None);
    }
}
