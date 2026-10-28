//! Per-phase request counters for the publication skeleton (ADR-3331
//! decision 4).
//!
//! Every store call the crate issues is counted once, under the phase that
//! issued it, when it is issued, whatever it returns:
//!
//! - `resolve`: GETs of HEAD (the first read, every re-read after a conflict
//!   or a retryable read failure, `read_head`, `resolve`) and the LIST of
//!   `list_agents`.
//! - `build`: the kernel transition and the dependency plan. It issues no
//!   store call, so its request figures stay zero; it is a phase so a
//!   snapshot shows that explicitly.
//! - `store`: dependency PUTs (`CreateIfAbsent`), the read-back GET after
//!   `AlreadyExists`, existence probes for referenced objects the command did
//!   not supply (a `head` call, counted under `get`), and `read_object`.
//! - `publish`: the HEAD write (`CreateIfAbsent` on create, counted under
//!   `put`; `CasVersion` otherwise, counted under `cas`) and the advisory
//!   `_dir` and `_wake` writes after it (under `put`).
//!
//! DELETEs of `_wake` hints (`AgentStore::delete_wake_hint`) belong to no
//! publication phase and [`PhaseCounts`] has no delete field, so they are
//! counted apart, by [`Counters::deletes`]. The two cursor listings
//! (`list_wake_hints`, `list_dir_after`) count under `resolve` `list`, like
//! `list_agents`.
//!
//! `conflict` counts conditional writes the store refused: `PreconditionFailed`
//! on a HEAD CAS and `AlreadyExists` on a HEAD create (publish phase), and
//! `AlreadyExists` on a dependency PUT (store phase, the normal idempotent
//! path for an object some earlier attempt already wrote).
//!
//! Byte figures count object bodies as handed to and returned by
//! [`ravel_object_store::ObjectStoreBackend`], not wire bytes: `bytes_out` is
//! the length of every PUT or CAS body the crate sent, `bytes_in` the length of
//! every GET body it received. Both include every retry and every failed
//! attempt whose body was sent or received, and neither includes headers,
//! signing, TLS or encryption overhead. A `head` probe moves no body bytes.

use std::sync::atomic::{AtomicU64, Ordering};

/// The four phases of the publication skeleton.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    Resolve,
    Build,
    Store,
    Publish,
}

/// The request kinds a phase counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Request {
    Get,
    Put,
    Cas,
    List,
}

#[derive(Debug, Default)]
struct PhaseCounters {
    get: AtomicU64,
    put: AtomicU64,
    cas: AtomicU64,
    list: AtomicU64,
    conflict: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

impl PhaseCounters {
    fn snapshot(&self) -> PhaseCounts {
        PhaseCounts {
            get: self.get.load(Ordering::Relaxed),
            put: self.put.load(Ordering::Relaxed),
            cas: self.cas.load(Ordering::Relaxed),
            list: self.list.load(Ordering::Relaxed),
            conflict: self.conflict.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        }
    }
}

/// The live counters of one [`crate::AgentStore`].
#[derive(Debug, Default)]
pub struct Counters {
    resolve: PhaseCounters,
    build: PhaseCounters,
    store: PhaseCounters,
    publish: PhaseCounters,
    deletes: AtomicU64,
}

impl Counters {
    fn phase(&self, phase: Phase) -> &PhaseCounters {
        match phase {
            Phase::Resolve => &self.resolve,
            Phase::Build => &self.build,
            Phase::Store => &self.store,
            Phase::Publish => &self.publish,
        }
    }

    pub(crate) fn request(&self, phase: Phase, request: Request) {
        let p = self.phase(phase);
        let counter = match request {
            Request::Get => &p.get,
            Request::Put => &p.put,
            Request::Cas => &p.cas,
            Request::List => &p.list,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Wake hint DELETEs issued, every attempt counted.
    pub fn deletes(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
    }

    pub(crate) fn conflict(&self, phase: Phase) {
        self.phase(phase).conflict.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn bytes_in(&self, phase: Phase, n: usize) {
        self.phase(phase)
            .bytes_in
            .fetch_add(n as u64, Ordering::Relaxed);
    }

    pub(crate) fn bytes_out(&self, phase: Phase, n: usize) {
        self.phase(phase)
            .bytes_out
            .fetch_add(n as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            resolve: self.resolve.snapshot(),
            build: self.build.snapshot(),
            store: self.store.snapshot(),
            publish: self.publish.snapshot(),
        }
    }
}

/// One phase's figures at a point in time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhaseCounts {
    pub get: u64,
    pub put: u64,
    pub cas: u64,
    pub list: u64,
    pub conflict: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl PhaseCounts {
    fn since(&self, earlier: &PhaseCounts) -> PhaseCounts {
        PhaseCounts {
            get: self.get.saturating_sub(earlier.get),
            put: self.put.saturating_sub(earlier.put),
            cas: self.cas.saturating_sub(earlier.cas),
            list: self.list.saturating_sub(earlier.list),
            conflict: self.conflict.saturating_sub(earlier.conflict),
            bytes_in: self.bytes_in.saturating_sub(earlier.bytes_in),
            bytes_out: self.bytes_out.saturating_sub(earlier.bytes_out),
        }
    }

    /// Requests of every kind, conflicts excluded.
    pub fn requests(&self) -> u64 {
        self.get + self.put + self.cas + self.list
    }
}

/// Every phase's figures at a point in time; what the runtime reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CounterSnapshot {
    pub resolve: PhaseCounts,
    pub build: PhaseCounts,
    pub store: PhaseCounts,
    pub publish: PhaseCounts,
}

impl CounterSnapshot {
    pub fn phase(&self, phase: Phase) -> &PhaseCounts {
        match phase {
            Phase::Resolve => &self.resolve,
            Phase::Build => &self.build,
            Phase::Store => &self.store,
            Phase::Publish => &self.publish,
        }
    }

    /// What moved between `earlier` and this snapshot.
    pub fn since(&self, earlier: &CounterSnapshot) -> CounterSnapshot {
        CounterSnapshot {
            resolve: self.resolve.since(&earlier.resolve),
            build: self.build.since(&earlier.build),
            store: self.store.since(&earlier.store),
            publish: self.publish.since(&earlier.publish),
        }
    }

    /// The sum over the four phases.
    pub fn total(&self) -> PhaseCounts {
        [self.resolve, self.build, self.store, self.publish]
            .iter()
            .fold(PhaseCounts::default(), |acc, p| PhaseCounts {
                get: acc.get + p.get,
                put: acc.put + p.put,
                cas: acc.cas + p.cas,
                list: acc.list + p.list,
                conflict: acc.conflict + p.conflict,
                bytes_in: acc.bytes_in + p.bytes_in,
                bytes_out: acc.bytes_out + p.bytes_out,
            })
    }
}
