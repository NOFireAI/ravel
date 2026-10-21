//! Ravel Kubernetes operator (ADR-0034).
//!
//! Reconciles a `RavelCluster` custom resource into gateway, query, and
//! maintain Deployments and Services for `services/ravel-server`. Its main
//! modules:
//!
//! - [`crd`]: the `RavelCluster` custom resource types and CRD generation.
//! - [`reconcile`]: pure functions that render a [`crd::RavelClusterSpec`] into
//!   Kubernetes objects. No I/O, no runtime; unit-tested directly.
//! - [`controller`]: the thin I/O layer that watches the resource, calls the
//!   pure functions, and applies the results.
//! - [`pod_identity`]: the EKS Pod Identity credentials sidecar
//!   (`ravel-operator pod-identity-credentials`) and the operator's own
//!   in-memory Pod Identity credentials, for `storage.s3.auth: podIdentity`.
//!
//! The operator renders Kubernetes objects from the CRD. Outside the API
//! server it talks to the object store, through `ravel-object-store`, only for
//! `sys/auth` and the reshard records, and under `podIdentity` to the node's
//! Pod Identity agent for its own credentials.

pub mod controller;
pub mod crd;
pub mod health;
pub mod metrics;
pub mod pod_identity;
pub mod reconcile;

pub use crd::{RavelCluster, RavelClusterSpec, RavelClusterStatus, ravel_cluster_crd};
