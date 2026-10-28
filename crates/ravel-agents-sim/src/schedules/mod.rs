//! Named schedules: the production store and runtime driven through the
//! scheduler and checked against the reference machine.

#![allow(clippy::expect_used)]

mod effects;
mod errors;
mod races;
mod senders;
mod storm;
mod timers;
mod workers;

use std::future::Future;
use std::sync::{Arc, OnceLock};

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op};
use ravel_sim::GateScript;
use serde_json::{Value, json};

use crate::drive::{Proc, Rig, Run};
use crate::scheduler::{
    Actor, ActorStore, Role, ScheduleError, ScheduleOutcome, ScheduleScript, Scheduler, ScriptStep,
};
use crate::seeds::AgentSeeds;

/// Seeds in the default batch.
pub(crate) const BATCH: u64 = 64;
/// First seed of the default batch.
pub(crate) const BASE: u64 = 0x5eed_0000;
/// First seed of a batch, in decimal.
pub(crate) const SEED_ENV: &str = "RAVEL_SIM_SEED";
/// Seeds in a batch, in decimal.
pub(crate) const SEEDS_ENV: &str = "RAVEL_SIM_SEEDS";

/// The seeds every batch runs: `count` consecutive seeds from `base`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Batch {
    pub(crate) base: u64,
    pub(crate) count: u64,
}

impl Batch {
    /// `RAVEL_SIM_SEED` and `RAVEL_SIM_SEEDS`, each defaulting to the
    /// default batch's when unset. A value that does not parse, or a count
    /// of zero, panics rather than running a batch nobody asked for.
    pub(crate) fn from_env() -> Batch {
        Batch::parse(
            std::env::var(SEED_ENV).ok().as_deref(),
            std::env::var(SEEDS_ENV).ok().as_deref(),
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    fn parse(base: Option<&str>, count: Option<&str>) -> Result<Batch, String> {
        let read = |name: &str, raw: Option<&str>, default: u64| match raw {
            None => Ok(default),
            Some(v) => v
                .trim()
                .parse::<u64>()
                .map_err(|e| format!("{name}={v:?} is not a decimal u64: {e}")),
        };
        let batch = Batch {
            base: read(SEED_ENV, base, BASE)?,
            count: read(SEEDS_ENV, count, BATCH)?,
        };
        if batch.count == 0 {
            return Err(format!("{SEEDS_ENV}=0 runs no seed"));
        }
        Ok(batch)
    }

    /// Whether this is the batch the pinned totals were measured on.
    pub(crate) fn is_default(&self) -> bool {
        *self
            == Batch {
                base: BASE,
                count: BATCH,
            }
    }

    pub(crate) fn seeds(&self) -> impl Iterator<Item = u64> + use<> {
        let base = self.base;
        (0..self.count).map(move |i| base.wrapping_add(i))
    }

    /// The line [`pinned`] prints for one schedule's batch.
    fn line<T: std::fmt::Debug + PartialEq>(&self, schedule: &str, totals: &T, pins: &T) -> String {
        let verdict = match (self.is_default(), totals == pins) {
            (true, true) => "match",
            (true, false) => "MISS",
            (false, _) => "unpinned",
        };
        format!(
            "sim-batch {schedule} base={} seeds={} totals={totals:?} pinned={pins:?} {verdict}",
            self.base, self.count
        )
    }
}

/// Seeds every batch runs, from [`Batch::from_env`]. A failure message's
/// replay line sets `RAVEL_SIM_SEED` to the failing batch seed, so the
/// replayed batch runs that seed first.
pub(crate) fn seeds() -> impl Iterator<Item = u64> {
    Batch::from_env().seeds()
}

/// `BATCH` consecutive seeds starting at `base`.
pub(crate) fn seeds_from(base: u64) -> impl Iterator<Item = u64> {
    Batch { base, count: BATCH }.seeds()
}

/// Close a batch: `runs` must be the batch's seed count, and one line goes
/// to stdout naming the schedule, the seeds, the totals and the pins, which
/// `nightly.sh` collects. On the default batch the totals must equal the
/// pins; on any other the pins were never measured, so the line says
/// `unpinned` and only the per-seed checks the schedule already made apply.
pub(crate) fn pinned<T: std::fmt::Debug + PartialEq>(
    schedule: &str,
    runs: u64,
    totals: T,
    pins: T,
) {
    let batch = Batch::from_env();
    assert_eq!(runs, batch.count, "{schedule}: runs against the batch size");
    let line = batch.line(schedule, &totals, &pins);
    println!("{line}");
    if batch.is_default() {
        assert_eq!(totals, pins, "{line}");
    }
}

/// A client actor whose body runs against its own process.
pub(crate) fn client<F, Fut>(rig: &Rig, name: &str, id_seed: u64, body: F) -> Actor
where
    F: FnOnce(Proc) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let rig = rig.clone();
    let owned = name.to_string();
    Actor::client(name, move |store| {
        body(rig.client_proc(&owned, store, id_seed))
    })
}

/// A worker actor whose body runs against its own process.
pub(crate) fn worker<F, Fut>(rig: &Rig, name: &str, id_seed: u64, body: F) -> Actor
where
    F: FnOnce(Proc) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let rig = rig.clone();
    let owned = name.to_string();
    Actor::worker(name, move |store| body(rig.proc(&owned, store, id_seed)))
}

/// The fault layer an actor built from [`faulty`] put under its process.
pub(crate) type Faults = Arc<OnceLock<Arc<FaultStore<Arc<dyn ObjectStoreBackend>>>>>;

/// An actor whose process sees `plan` applied to its own calls only, under
/// its scheduler gates. The returned cell holds that layer once the actor
/// starts, so the schedule can count the faults that fired.
pub(crate) fn faulty<F, Fut>(
    rig: &Rig,
    role: Role,
    name: &str,
    id_seed: u64,
    plan: FaultPlan,
    body: F,
) -> (Actor, Faults)
where
    F: FnOnce(Proc) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let rig = rig.clone();
    let owned = name.to_string();
    let faults: Faults = Arc::default();
    let cell = Arc::clone(&faults);
    let body = move |store: ActorStore| {
        let layer = Arc::new(FaultStore::new(store as Arc<dyn ObjectStoreBackend>, plan));
        let _ = cell.set(Arc::clone(&layer));
        body(match role {
            Role::Client => rig.client_proc(&owned, layer, id_seed),
            Role::Worker => rig.proc(&owned, layer, id_seed),
        })
    };
    let actor = match role {
        Role::Client => Actor::client(name, body),
        Role::Worker => Actor::worker(name, body),
    };
    (actor, faults)
}

/// Every fault an actor's layer injected, by op and kind.
pub(crate) fn all_fired(faults: &Faults) -> Vec<((Op, FaultKind), u64)> {
    let mut all: Vec<_> = faults
        .get()
        .map(|f| {
            f.counters_snapshot()
                .into_iter()
                .filter(|(_, n)| *n > 0)
                .collect()
        })
        .unwrap_or_default();
    all.sort_by_key(|((op, kind), _)| (format!("{op:?}"), format!("{kind:?}")));
    all
}

pub(crate) fn run(
    rig: &Rig,
    seed: u64,
    script: ScheduleScript,
    actors: Vec<Actor>,
) -> Result<ScheduleOutcome, ScheduleError> {
    run_in_batch(rig, seed, seed, script, actors)
}

/// [`run`] on a seed derived from the batch seed `batch`, which failure
/// messages replay from.
pub(crate) fn run_in_batch(
    rig: &Rig,
    batch: u64,
    seed: u64,
    script: ScheduleScript,
    actors: Vec<Actor>,
) -> Result<ScheduleOutcome, ScheduleError> {
    Scheduler::new(AgentSeeds::new(seed), script)
        .with_batch_seed(batch)
        .run(rig.shared(), actors)
}

#[test]
fn seeds_from_counts_a_batch_up_from_its_base() {
    let batch: Vec<u64> = seeds_from(7).collect();
    assert_eq!(batch.len(), 64);
    assert_eq!(batch.first(), Some(&7));
    assert_eq!(batch.last(), Some(&70));
    assert!(batch.windows(2).all(|w| w[1] == w[0] + 1));
    assert_eq!(seeds_from(u64::MAX).nth(1), Some(0));
}

#[test]
fn a_batch_reads_its_base_and_count_and_refuses_what_does_not_parse() {
    assert_eq!(
        Batch::parse(None, None),
        Ok(Batch {
            base: BASE,
            count: BATCH
        })
    );
    assert!(Batch::parse(None, None).is_ok_and(|b| b.is_default()));
    let custom = Batch::parse(Some(" 7 "), Some("3")).expect("parses");
    assert_eq!(custom, Batch { base: 7, count: 3 });
    assert_eq!(custom.seeds().collect::<Vec<_>>(), vec![7, 8, 9]);
    assert!(!custom.is_default());
    assert!(Batch::parse(Some("0x5eed"), None).is_err());
    assert!(Batch::parse(None, Some("many")).is_err());
    assert!(Batch::parse(None, Some("0")).is_err());
}

#[test]
fn a_batch_line_names_its_verdict() {
    let default = Batch {
        base: BASE,
        count: BATCH,
    };
    assert_eq!(
        default.line("s", &3, &3),
        "sim-batch s base=1592590336 seeds=64 totals=3 pinned=3 match"
    );
    assert!(default.line("s", &3, &4).ends_with(" MISS"));
    let other = Batch { base: 1, count: 2 };
    assert_eq!(
        other.line("s", &3, &4),
        "sim-batch s base=1 seeds=2 totals=3 pinned=4 unpinned"
    );
}

/// A script from `seed` over `actors` actors.
pub(crate) fn seeded(seed: u64, actors: usize, len: usize, crash_per_mille: u32) -> ScheduleScript {
    ScheduleScript::from_seed(&AgentSeeds::new(seed), actors, len, crash_per_mille)
}

/// The same script with every crash of an actor in `clients` turned into a
/// release: only workers die.
pub(crate) fn crash_workers_only(script: ScheduleScript, clients: &[usize]) -> ScheduleScript {
    ScheduleScript::new(
        script
            .steps
            .into_iter()
            .map(|step| match step {
                ScriptStep::Crash(i) if clients.contains(&i) => ScriptStep::Release(i),
                other => other,
            })
            .collect(),
    )
}

/// A gate holding the `n`-th call of `op` whose key contains `key`.
pub(crate) fn gate(op: Op, key: &str, n: u64) -> GateScript {
    GateScript {
        op,
        key_contains: Some(key.to_string()),
        occurrence: Occurrence::Nth(n),
    }
}

/// Create the agent outside any schedule.
pub(crate) fn create(rig: &Rig) {
    rig.setup(|p| async move {
        p.create().await.map_err(|e| format!("create: {e:?}"))?;
        Ok(())
    })
    .expect("setup create");
}

/// Create the agent and send `r1..=rn` outside any schedule.
pub(crate) fn create_with_inputs(rig: &Rig, n: u64) {
    rig.setup(|p| async move {
        p.create().await.map_err(|e| format!("create: {e:?}"))?;
        for i in 1..=n {
            p.send(&format!("r{i}"), &payload(i))
                .await
                .map_err(|e| format!("send r{i}: {e:?}"))?;
        }
        Ok(())
    })
    .expect("setup inputs");
}

/// Name of the replacement worker [`drain`] runs.
pub(crate) const DRAIN: &str = "drain";

/// Put every lease past its grace, then run a fresh worker on its own
/// coordinator until it finds no work.
pub(crate) fn drain(rig: &Rig, seed: u64) -> ScheduleOutcome {
    rig.clock.advance_ms(crate::drive::PAST_LEASE_MS);
    let w = worker(rig, DRAIN, 99, |p| async move {
        p.work(16).await;
        Ok(())
    });
    run(rig, seed, ScheduleScript::new(Vec::new()), vec![w]).expect("drain")
}

pub(crate) fn payload(n: u64) -> Value {
    json!({ "n": n })
}

/// Build the run and check it: nothing unmappable, and the history passes
/// the reference machine.
pub(crate) fn checked(rig: &Rig, ctx: &dyn crate::scheduler::FailureContext) -> Run {
    let run = rig.build().expect("build history");
    if !run.unmappable.is_empty() {
        panic!(
            "unmappable: {:?}\n{}",
            run.unmappable,
            ctx.failure_context()
        );
    }
    if let Err(divergence) = run.check() {
        panic!(
            "history diverged: {divergence}\nhistory: {:?}\n{}",
            run.history,
            ctx.failure_context()
        );
    }
    run
}
