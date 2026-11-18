//! A seeded poller that interleaves several futures at store-call granularity,
//! for use with a yielding [`super::tap::TapStore`]: every store call returns
//! `Pending` once, and the seed picks which task to poll next.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

thread_local! {
    /// The task index `drive` is currently polling, for a tap store to tag
    /// the calls it receives with. `None` outside a `drive`/`block_on` poll.
    static CURRENT_TASK: Cell<Option<usize>> = const { Cell::new(None) };
}

/// The task index the calling store call was entered under, if a `drive` poll
/// is on the stack. Single-threaded, cooperative polling makes this exact:
/// only one task's future ever runs between two calls into a store.
pub fn current_task() -> Option<usize> {
    CURRENT_TASK.with(Cell::get)
}

/// Polls before a schedule counts as stuck; a full schedule takes a few
/// hundred.
const MAX_POLLS: usize = 200_000;
/// `Pending` returns in a row, from one task, without a store call between
/// them: past this the task waits on something no store call will release.
const MAX_IDLE_POLLS: usize = 1_000;

/// SplitMix64: a fixed, dependency-free stream per seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % n as u64).expect("fits")
    }
}

pub type Task<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Poll `tasks` to completion in the order `seed` picks, and return each
/// task's output and the order the tasks finished in. `calls` counts the
/// store calls the tasks have entered.
///
/// Polling happens outside any tokio runtime on purpose: inside one, every
/// poll here runs within a single runtime task that never yields to the
/// runtime, so tokio's cooperative budget runs out and the build's fetch
/// semaphore returns `Pending` for good.
pub fn drive<T>(
    seed: u64,
    calls: &dyn Fn() -> usize,
    mut tasks: Vec<Task<'_, T>>,
) -> (Vec<T>, Vec<usize>) {
    let mut cx = Context::from_waker(Waker::noop());
    let mut rng = Rng(seed);
    let mut out: Vec<Option<T>> = tasks.iter().map(|_| None).collect();
    let mut live: Vec<usize> = (0..tasks.len()).collect();
    let mut finished = Vec::new();
    let mut idle = vec![0usize; tasks.len()];
    let mut polls = 0usize;
    while !live.is_empty() {
        polls += 1;
        assert!(
            polls < MAX_POLLS,
            "seed {seed}: no progress after {polls} polls, live tasks {live:?}"
        );
        let pick = rng.below(live.len());
        let task = live[pick];
        let before = calls();
        CURRENT_TASK.with(|c| c.set(Some(task)));
        let polled = tasks[task].as_mut().poll(&mut cx);
        CURRENT_TASK.with(|c| c.set(None));
        match polled {
            Poll::Ready(done) => {
                out[task] = Some(done);
                live.remove(pick);
                finished.push(task);
            }
            Poll::Pending if calls() > before => idle[task] = 0,
            Poll::Pending => {
                idle[task] += 1;
                assert!(
                    idle[task] < MAX_IDLE_POLLS,
                    "seed {seed}: task {task} returned Pending {MAX_IDLE_POLLS} times in a row \
                     without a store call, after {polls} polls"
                );
            }
        }
    }
    let out = out.into_iter().map(|d| d.expect("finished")).collect();
    (out, finished)
}

/// Run one future to completion with the same poller.
pub fn block_on<T>(seed: u64, future: impl Future<Output = T>) -> T {
    let (mut out, _) = drive(seed, &|| 0, vec![Box::pin(future) as Task<'_, T>]);
    out.pop().expect("one output")
}
