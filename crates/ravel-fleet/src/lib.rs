//! Fleet-membership primitives shared across worker roles.
//!
//! This crate holds the object-store-mediated membership machinery that was
//! first built for the maintain role (ADR-0065 decisions 1 and 2) and is now
//! reused by the distributed read fan-out's query workers (ADR-0071). There is
//! no leader, no assignment object, and no new durable state: each process
//! writes a heartbeat key named by its own process id, lists the sibling
//! prefix to compute a live set by a staleness window, and (for maintain)
//! partitions work by rendezvous hashing over that live set. The role's IAM
//! grant lets any holder write any key under the prefix, so a reader admits
//! a sibling only when its record MAC verifies under a record key
//! ([`record_auth`]; ADR-1918 for query workers, ADR-3130 for maintain).
//!
//! - [`worker_set`] is the maintain-role membership and rendezvous-ownership
//!   logic, relocated from `ravel-maintain` (which re-exports it at its former
//!   paths). Since ADR-3130 its records carry a MAC, and writing or reading
//!   them takes a [`worker_set::MaintainMembership`] built from record keys.
//! - [`query_workers`] is the query-worker registration for the ADR-0071 read
//!   fan-out: the same single-writer `Overwrite` heartbeat shape and the same
//!   staleness rule, over the `sys/query/workers/` prefix.
//! - [`claim`] is the ADR-1029 advisory work claim: a CAS-mutated object per
//!   unit of expensive maintenance work, suppressing duplicate merges in the
//!   windows rendezvous ownership cannot reach. It is neither membership nor a
//!   lease; see its module docs for the three-way disambiguation.

pub mod claim;
pub mod query_workers;
pub mod record_auth;
pub mod worker_set;
