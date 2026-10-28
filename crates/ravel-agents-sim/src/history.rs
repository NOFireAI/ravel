//! Recorded client histories and the checker that replays them through the
//! [`ReferenceMachine`].
//!
//! A history is a sequence of [`Call`]s, each an [`Event`] and the reply the
//! system under test was observed to give. The recorder appends calls in the
//! order their effects were linearized by the store: a client call at the
//! point its CAS or read returned, a turn event at its commit. The checker
//! replays that sequence through a fresh machine and compares reply for
//! reply, so a history passes exactly when it is sequentially consistent
//! with the recorded order, and accept order is whatever order the
//! accepting calls appear in.

use std::fmt;

use crate::reference::{Event, ReferenceMachine, Reply};

/// One recorded call and the reply the system gave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub event: Event,
    pub observed: Reply,
}

/// A recorded history of one agent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    calls: Vec<Call>,
}

/// The first call whose observed reply differs from the reference machine's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    /// Position of the call in the history, from 0.
    pub index: usize,
    pub event: Event,
    pub expected: Reply,
    pub observed: Reply,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "history diverges at call {}: event {:?}: reference expected {:?}, observed {:?}",
            self.index, self.event, self.expected, self.observed
        )
    }
}

impl std::error::Error for Divergence {}

impl History {
    /// Append a call in linearization order.
    pub fn record(&mut self, event: Event, observed: Reply) {
        self.calls.push(Call { event, observed });
    }

    pub fn calls(&self) -> &[Call] {
        &self.calls
    }

    pub fn len(&self) -> usize {
        self.calls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// Replay the history through `machine` and return the first divergence.
    /// `machine` is the agent's state before the first call, normally a
    /// fresh [`ReferenceMachine::new`].
    pub fn check(&self, mut machine: ReferenceMachine) -> Result<(), Divergence> {
        for (index, call) in self.calls.iter().enumerate() {
            let expected = machine.apply(&call.event);
            if expected != call.observed {
                return Err(Divergence {
                    index,
                    event: call.event.clone(),
                    expected,
                    observed: call.observed.clone(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::reference::ClientOp;

    #[test]
    fn the_first_divergence_is_reported_and_later_ones_are_not() {
        let machine = ReferenceMachine::new("support", "a").expect("valid agent name");
        let mut history = History::default();
        history.record(
            Event::Client(ClientOp::Create),
            Reply::Created {
                incarnation: 1,
                created: true,
            },
        );
        history.record(
            Event::Client(ClientOp::State),
            Reply::State {
                incarnation: 1,
                state_version: 3,
            },
        );
        history.record(
            Event::Client(ClientOp::State),
            Reply::State {
                incarnation: 9,
                state_version: 9,
            },
        );
        let divergence = history.check(machine).expect_err("diverges");
        assert_eq!(divergence.index, 1);
        assert_eq!(
            divergence.expected,
            Reply::State {
                incarnation: 1,
                state_version: 0,
            }
        );
        let message = divergence.to_string();
        assert!(message.contains("call 1"), "{message}");
        assert!(message.contains("state_version: 3"), "{message}");
        assert!(message.contains("state_version: 0"), "{message}");
    }
}
