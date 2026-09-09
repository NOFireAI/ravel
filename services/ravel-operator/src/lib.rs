//! Ravel Kubernetes operator (ADR-0034).
//!
//! Reconciles a `RavelCluster` custom resource into gateway, query, and
//! maintain Deployments and Services for `services/ravel-server`. The crate is
//! split into three layers:
//!
//! - [`crd`]: the `RavelCluster` custom resource types and CRD generation.
//! - [`reconcile`]: pure functions that render a [`crd::RavelClusterSpec`] into
//!   Kubernetes objects. No I/O, no runtime; unit-tested directly.
//! - [`controller`]: the thin I/O layer that watches the resource, calls the
//!   pure functions, and applies the results.
//!
//! The operator renders Kubernetes objects from the CRD only. It never talks to
//! the object store or links any `ravel-*` crate.

pub mod controller;
pub mod crd;
pub mod reconcile;

pub use crd::{RavelCluster, RavelClusterSpec, RavelClusterStatus, ravel_cluster_crd};
