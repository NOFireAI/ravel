//! Horizontal autoscaling of the gateway, query and maintain tiers
//! (ADR-4314): which tiers are handed to a scaler, the replica count each
//! pass applies, the HPA the operator renders in `Managed` mode, and the
//! status bookkeeping that keeps the live count when autoscaling is turned
//! off.
//!
//! Everything here is pure; the controller reads the live Deployment, writes
//! status and calls the API in the order [`TierPlan`] describes.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::autoscaling::v2::{
    CrossVersionObjectReference, HPAScalingPolicy, HPAScalingRules, HorizontalPodAutoscaler,
    HorizontalPodAutoscalerBehavior, HorizontalPodAutoscalerSpec, MetricSpec, MetricTarget,
    ResourceMetricSource,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde_json::{Value, json};

use crate::crd::{
    AffinityBackend, AutoscaledTier, AutoscalingMode, AutoscalingSpec, RavelClusterSpec,
    RavelClusterStatus, ReplicaAdoption, ResourceRequirementsSpec, ScalableTier,
};
use crate::reconcile::{DeploymentTier, child_name, labels};

/// The field manager that keeps `spec.replicas` owned while the operator's
/// own manager releases it (ADR-4314 decision 1, step 1).
pub const HANDOVER_FIELD_MANAGER: &str = "ravel-operator-handover";

/// The field manager the operator applies its objects under.
const OPERATOR_FIELD_MANAGER: &str = "ravel-operator";

/// The `managed-by` label value every object the operator renders carries
/// ([`crate::reconcile::labels`]).
const MANAGED_BY_OPERATOR: &str = "ravel-operator";

const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const INSTANCE_LABEL: &str = "app.kubernetes.io/instance";

/// Scale-down period for the gateway and query HPAs: one pod per 60 s, above
/// the longest termination grace period the operator renders
/// ([`crate::reconcile::POD_TERMINATION_GRACE_PERIOD_WITH_HEALTH_PORT_SECONDS`],
/// 51 s).
pub const REQUEST_SERVING_SCALE_DOWN_PERIOD_SECONDS: i32 = 60;

/// Scale-down period for the maintain HPA: one pod per 600 s, above the 570 s
/// fold handover bound (docs/guides/operations/maintenance.md).
pub const MAINTAIN_SCALE_DOWN_PERIOD_SECONDS: i32 = 600;

impl From<DeploymentTier> for ScalableTier {
    fn from(tier: DeploymentTier) -> Self {
        match tier {
            DeploymentTier::Gateway => Self::Gateway,
            DeploymentTier::Query => Self::Query,
            DeploymentTier::Maintain => Self::Maintain,
        }
    }
}

/// The tier's `autoscaling` block, if set.
pub fn tier_autoscaling(spec: &RavelClusterSpec, tier: DeploymentTier) -> Option<&AutoscalingSpec> {
    match tier {
        DeploymentTier::Gateway => spec.gateway.autoscaling.as_ref(),
        DeploymentTier::Query => spec.query.autoscaling.as_ref(),
        DeploymentTier::Maintain => spec.maintain.autoscaling.as_ref(),
    }
}

/// The tier's declared `replicas`.
pub fn tier_replicas(spec: &RavelClusterSpec, tier: DeploymentTier) -> i32 {
    match tier {
        DeploymentTier::Gateway => spec.gateway.replicas,
        DeploymentTier::Query => spec.query.replicas,
        DeploymentTier::Maintain => spec.maintain.replicas,
    }
}

fn tier_resources(
    spec: &RavelClusterSpec,
    tier: DeploymentTier,
) -> Option<&ResourceRequirementsSpec> {
    match tier {
        DeploymentTier::Gateway => spec.gateway.resources.as_ref(),
        DeploymentTier::Query => spec.query.resources.as_ref(),
        DeploymentTier::Maintain => spec.maintain.resources.as_ref(),
    }
}

/// Why autoscaling is withheld for a tier (ADR-4314 decision 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithheldReason {
    /// `Managed` mode on a tier whose pods carry no CPU request.
    NoCpuRequest,
    /// `Managed` mode with no `targetCPUUtilizationPercentage`, which the
    /// CRD refuses at admission.
    NoTarget,
    /// The gateway with ingest affinity enabled on `backend: ingressNginx`.
    IngressNginxAffinity,
    /// `Managed` mode while an HPA this RavelCluster does not own
    /// ([`HpaListing`]) holds the tier's HPA name. The operator would have to
    /// take that HPA over to render its own.
    HpaNameTaken,
}

impl WithheldReason {
    fn message(self) -> &'static str {
        match self {
            Self::NoCpuRequest => {
                "mode Managed needs a CPU request on the tier, since the HPA \
                 measures utilization against it"
            }
            Self::NoTarget => "mode Managed needs targetCPUUtilizationPercentage",
            Self::IngressNginxAffinity => {
                "ingestAffinity is enabled on backend ingressNginx, which \
                 regroups every tenant subset on each replica count change"
            }
            Self::HpaNameTaken => {
                "mode Managed renders its HorizontalPodAutoscaler at the tier's \
                 Deployment name, and an HPA this RavelCluster does not own \
                 already exists there; the operator leaves that HPA as it is. \
                 Delete it to use mode Managed, or use mode External to keep it"
            }
        }
    }
}

/// Whether the tier's pods carry a CPU request. An omitted `resources` block
/// renders the default request, which has one. A CPU limit without a request
/// counts too: the API server copies the limit into the request.
fn has_cpu_request(resources: Option<&ResourceRequirementsSpec>) -> bool {
    let Some(resources) = resources else {
        return true;
    };
    [&resources.requests, &resources.limits]
        .into_iter()
        .any(|m| m.as_ref().is_some_and(|m| m.contains_key("cpu")))
}

/// Why the tier's `autoscaling` block is withheld, or `None` when the tier
/// has no block or the block can apply.
pub fn withheld_reason(spec: &RavelClusterSpec, tier: DeploymentTier) -> Option<WithheldReason> {
    let autoscaling = tier_autoscaling(spec, tier)?;
    let ingress_nginx = tier == DeploymentTier::Gateway
        && spec
            .gateway
            .ingest_affinity
            .as_ref()
            .is_some_and(|a| a.enabled && matches!(a.backend, AffinityBackend::IngressNginx));
    if ingress_nginx {
        return Some(WithheldReason::IngressNginxAffinity);
    }
    if autoscaling.mode != AutoscalingMode::Managed {
        return None;
    }
    if !has_cpu_request(tier_resources(spec, tier)) {
        return Some(WithheldReason::NoCpuRequest);
    }
    if autoscaling.target_cpu_utilization_percentage.is_none() {
        return Some(WithheldReason::NoTarget);
    }
    None
}

/// [`withheld_reason`], plus [`WithheldReason::HpaNameTaken`] for a `Managed`
/// tier the spec does not withhold when `hpa_name_taken` (an HPA this
/// RavelCluster does not own sits at the tier's HPA name).
pub fn tier_withheld_reason(
    spec: &RavelClusterSpec,
    tier: DeploymentTier,
    hpa_name_taken: bool,
) -> Option<WithheldReason> {
    withheld_reason(spec, tier).or_else(|| {
        let managed =
            tier_autoscaling(spec, tier).is_some_and(|a| a.mode == AutoscalingMode::Managed);
        (managed && hpa_name_taken).then_some(WithheldReason::HpaNameTaken)
    })
}

pub(crate) const TIERS: [DeploymentTier; 3] = [
    DeploymentTier::Gateway,
    DeploymentTier::Query,
    DeploymentTier::Maintain,
];

/// The `AutoscalingWithheld` message naming every withheld tier and its
/// reason, or `None` when no tier is withheld. `hpa_name_taken` lists the
/// tiers whose HPA name an HPA this RavelCluster does not own holds.
pub fn withheld_message(
    spec: &RavelClusterSpec,
    hpa_name_taken: &[DeploymentTier],
) -> Option<String> {
    let parts: Vec<String> = TIERS
        .into_iter()
        .filter_map(|tier| {
            tier_withheld_reason(spec, tier, hpa_name_taken.contains(&tier))
                .map(|reason| format!("{}: {}", tier.component(), reason.message()))
        })
        .collect();
    (!parts.is_empty()).then(|| {
        format!(
            "autoscaling withheld, the operator manages the replica count itself (the declared \
             replicas, or the count adopted when the tier was autoscaled before) and renders no \
             HorizontalPodAutoscaler for: {}",
            parts.join("; ")
        )
    })
}

/// What the planner needs from the tier's live Deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveTier {
    /// The live `spec.replicas`.
    pub replicas: Option<i32>,
    /// Whether `ravel-operator` owns `spec.replicas` in `managedFields`.
    pub operator_owns_replicas: bool,
}

impl LiveTier {
    /// Read a live Deployment. With no `managedFields` at all the operator is
    /// taken to own the field: the handover is then a same-value apply,
    /// which is harmless when it does not.
    pub fn of(deployment: &Deployment) -> Self {
        let operator_owns_replicas = match deployment.metadata.managed_fields.as_deref() {
            None | Some([]) => true,
            Some(entries) => entries.iter().any(|entry| {
                entry.manager.as_deref() == Some(OPERATOR_FIELD_MANAGER)
                    && entry.subresource.as_deref().unwrap_or("").is_empty()
                    && entry
                        .fields_v1
                        .as_ref()
                        .and_then(|f| f.0.get("f:spec"))
                        .and_then(|s| s.get("f:replicas"))
                        .is_some()
            }),
        };
        Self {
            replicas: deployment.spec.as_ref().and_then(|s| s.replicas),
            operator_owns_replicas,
        }
    }
}

/// The `spec.replicas` the operator applies for a tier this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replicas {
    /// Apply `spec.replicas` at this count, with force.
    Fixed(i32),
    /// Apply the Deployment without `spec.replicas`. When `handover` is set,
    /// first apply that count under [`HANDOVER_FIELD_MANAGER`], without force.
    Released {
        /// The live count to hand over, when the operator still owns the field.
        handover: Option<i32>,
    },
}

/// One tier's autoscaling decision for one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierPlan {
    /// The replica count to apply, or the release.
    pub replicas: Replicas,
    /// Render the `Managed` HPA after the Deployment apply.
    pub render_hpa: bool,
    /// Delete the tier's HPA, if this RavelCluster owns it, before the
    /// Deployment apply.
    pub delete_hpa: bool,
    /// The tier's `autoscaledTiers` entry after this pass.
    pub autoscaled: Option<AutoscaledTier>,
    /// The tier's `replicaAdoptions` record after this pass.
    pub adoption: Option<ReplicaAdoption>,
    /// Persist `adoption` to status before any apply for the tier: the pass
    /// fails for the tier if that write fails.
    pub record_first: bool,
}

/// Why [`plan_tier`] cannot produce a plan for the tier this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    /// The tier's live Deployment is owned by `ravel-operator` on
    /// `spec.replicas` but carries no count, so the handover has nothing to
    /// transfer. The operator always applies the field, so a live object
    /// owning it yet missing it is corrupt; the tier errors rather than hand
    /// over an invented count (which would otherwise drop the tier to 1).
    HandoverReplicasMissing,
}

/// Whether [`plan_tier`] reads the live Deployment for this tier.
pub fn needs_live(
    spec: &RavelClusterSpec,
    tier: DeploymentTier,
    entry: Option<&AutoscaledTier>,
) -> bool {
    tier_autoscaling(spec, tier).is_some() || entry.is_some()
}

/// Decide one tier's replicas, HPA and status bookkeeping (ADR-4314
/// decisions 1 and 2). `live` is the tier's live Deployment, `None` when it
/// does not exist; it is read only when [`needs_live`] holds.
/// `hpa_name_taken` withholds a `Managed` tier ([`tier_withheld_reason`]).
pub fn plan_tier(
    spec: &RavelClusterSpec,
    tier: DeploymentTier,
    entry: Option<&AutoscaledTier>,
    adoption: Option<&ReplicaAdoption>,
    live: Option<LiveTier>,
    hpa_name_taken: bool,
) -> Result<TierPlan, PlanError> {
    let scalable = ScalableTier::from(tier);
    let declared = tier_replicas(spec, tier);
    let autoscaling = tier_autoscaling(spec, tier);
    let block_set = autoscaling.is_some();
    let active = autoscaling.filter(|_| tier_withheld_reason(spec, tier, hpa_name_taken).is_none());

    if let Some(autoscaling) = active {
        let autoscaled = Some(AutoscaledTier {
            tier: scalable,
            declared_replicas: declared,
        });
        let managed = autoscaling.mode == AutoscalingMode::Managed;
        return match live {
            // A handover patch cannot create a Deployment, so the first one is
            // created at minReplicas with the operator's own manager. ADR step
            // 0: stop for the tier here and record it only after the handover,
            // so `autoscaled` stays `None` on the create pass.
            None => Ok(TierPlan {
                replicas: Replicas::Fixed(autoscaling.min_replicas),
                render_hpa: false,
                delete_hpa: !managed,
                autoscaled: None,
                adoption: None,
                record_first: false,
            }),
            Some(live) => {
                let handover = if live.operator_owns_replicas {
                    // The operator owns the field, so it must have a count to
                    // hand over; a missing one is a corrupt live object.
                    Some(live.replicas.ok_or(PlanError::HandoverReplicasMissing)?)
                } else {
                    None
                };
                Ok(TierPlan {
                    replicas: Replicas::Released { handover },
                    render_hpa: managed,
                    delete_hpa: !managed,
                    autoscaled,
                    adoption: None,
                    record_first: false,
                })
            }
        };
    }

    let fixed = |replicas: i32, adoption: Option<ReplicaAdoption>, record_first: bool| {
        Ok(TierPlan {
            replicas: Replicas::Fixed(replicas),
            render_hpa: false,
            delete_hpa: block_set || entry.is_some(),
            autoscaled: None,
            adoption,
            record_first,
        })
    };

    if let Some(entry) = entry {
        // Turn-off. A `replicas` edited in the same change wins at once.
        if declared != entry.declared_replicas {
            return fixed(declared, None, false);
        }
        return match live.and_then(|l| l.replicas) {
            Some(live_replicas) => fixed(
                live_replicas,
                Some(ReplicaAdoption {
                    tier: scalable,
                    adopted_replicas: live_replicas,
                    declared_replicas: entry.declared_replicas,
                }),
                true,
            ),
            None => fixed(declared, None, false),
        };
    }

    match adoption {
        Some(record) if record.declared_replicas == declared => {
            fixed(record.adopted_replicas, Some(record.clone()), false)
        }
        _ => fixed(declared, None, false),
    }
}

/// The `autoscaledTiers` and `replicaAdoptions` the pass carries, seeded from
/// the stored status and updated tier by tier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AutoscalingState {
    autoscaled: Vec<AutoscaledTier>,
    adoptions: Vec<ReplicaAdoption>,
    /// The tiers this pass withholds with [`WithheldReason::HpaNameTaken`],
    /// for the `AutoscalingWithheld` condition. Observed each pass, never
    /// persisted.
    hpa_name_taken: Vec<DeploymentTier>,
}

impl AutoscalingState {
    /// The state a stored status records.
    pub fn from_status(status: Option<&RavelClusterStatus>) -> Self {
        Self {
            autoscaled: status
                .and_then(|s| s.autoscaled_tiers.clone())
                .unwrap_or_default(),
            adoptions: status
                .and_then(|s| s.replica_adoptions.clone())
                .unwrap_or_default(),
            hpa_name_taken: Vec::new(),
        }
    }

    /// Record the tiers this pass finds their HPA name taken on.
    pub fn set_hpa_name_taken(&mut self, tiers: Vec<DeploymentTier>) {
        self.hpa_name_taken = tiers;
    }

    /// Whether this pass found the tier's HPA name taken.
    pub fn hpa_name_taken(&self, tier: DeploymentTier) -> bool {
        self.hpa_name_taken.contains(&tier)
    }

    /// The `AutoscalingWithheld` message for this pass ([`withheld_message`]).
    pub fn withheld_message(&self, spec: &RavelClusterSpec) -> Option<String> {
        withheld_message(spec, &self.hpa_name_taken)
    }

    /// The tier's `autoscaledTiers` entry.
    pub fn entry(&self, tier: DeploymentTier) -> Option<&AutoscaledTier> {
        let tier = ScalableTier::from(tier);
        self.autoscaled.iter().find(|e| e.tier == tier)
    }

    /// The tier's adoption record.
    pub fn adoption(&self, tier: DeploymentTier) -> Option<&ReplicaAdoption> {
        let tier = ScalableTier::from(tier);
        self.adoptions.iter().find(|r| r.tier == tier)
    }

    /// Replace the tier's entry and record.
    pub fn set(
        &mut self,
        tier: DeploymentTier,
        autoscaled: Option<AutoscaledTier>,
        adoption: Option<ReplicaAdoption>,
    ) {
        let tier = ScalableTier::from(tier);
        self.autoscaled.retain(|e| e.tier != tier);
        self.autoscaled.extend(autoscaled);
        self.autoscaled.sort_by_key(|e| e.tier);
        self.adoptions.retain(|r| r.tier != tier);
        self.adoptions.extend(adoption);
        self.adoptions.sort_by_key(|r| r.tier);
    }

    /// `status.autoscaledTiers`: `None` when empty, which clears it.
    pub fn autoscaled_tiers(&self) -> Option<Vec<AutoscaledTier>> {
        (!self.autoscaled.is_empty()).then(|| self.autoscaled.clone())
    }

    /// `status.replicaAdoptions`: `None` when empty, which clears it.
    pub fn replica_adoptions(&self) -> Option<Vec<ReplicaAdoption>> {
        (!self.adoptions.is_empty()).then(|| self.adoptions.clone())
    }

    /// The status merge patch that persists the adoption records alone.
    pub fn adoptions_patch(&self) -> Value {
        json!({ "status": { "replicaAdoptions": self.replica_adoptions() } })
    }
}

/// The apply body that hands `spec.replicas` over at `replicas`: only the
/// identity and that one field, so the handover manager owns nothing else.
pub fn handover_patch(name: &str, namespace: &str, replicas: i32) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name, "namespace": namespace },
        "spec": { "replicas": replicas },
    })
}

/// The tier's HPA name: the Deployment's.
pub fn hpa_name(instance: &str, tier: DeploymentTier) -> String {
    child_name(instance, tier.component())
}

/// Whether this RavelCluster owns an HPA: the `managed-by` label set to
/// `ravel-operator`, the `instance` label set to `instance`, and an
/// ownerReference whose uid is the RavelCluster's `cluster_uid`. Every HPA
/// [`desired_hpa`] renders carries all three. The labels alone are not
/// enough: a copy of a rendered HPA saved under another name keeps them.
pub fn hpa_is_operator_owned(meta: &ObjectMeta, instance: &str, cluster_uid: &str) -> bool {
    let labelled = meta.labels.as_ref().is_some_and(|labels| {
        labels.get(MANAGED_BY_LABEL).map(String::as_str) == Some(MANAGED_BY_OPERATOR)
            && labels.get(INSTANCE_LABEL).map(String::as_str) == Some(instance)
    });
    let owned_by_cluster = meta
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|owner| owner.uid == cluster_uid);
    labelled && owned_by_cluster
}

/// The HPA at a tier's HPA name, as [`HpaListing`] read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HpaAtName {
    /// No HPA has the name.
    Absent,
    /// This RavelCluster owns the HPA ([`hpa_is_operator_owned`]).
    Owned,
    /// Some other HPA has the name: the user's, or one whose ownership marks
    /// do not name this RavelCluster.
    Foreign,
}

/// The namespace's HorizontalPodAutoscalers as one pass's list read them: each
/// name and whether this RavelCluster owns it. Every HPA delete, and the check
/// before a `Managed` HPA apply, is decided from this, never from the name
/// alone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HpaListing {
    by_name: BTreeMap<String, bool>,
}

impl HpaListing {
    /// The listing of `hpas` for the RavelCluster `instance` with uid
    /// `cluster_uid`. With no uid nothing is owned.
    pub fn new<'a>(
        hpas: impl IntoIterator<Item = &'a ObjectMeta>,
        instance: &str,
        cluster_uid: Option<&str>,
    ) -> Self {
        let by_name = hpas
            .into_iter()
            .filter_map(|meta| {
                let owned =
                    cluster_uid.is_some_and(|uid| hpa_is_operator_owned(meta, instance, uid));
                meta.name.clone().map(|name| (name, owned))
            })
            .collect();
        Self { by_name }
    }

    /// What holds `name`.
    pub fn at(&self, name: &str) -> HpaAtName {
        match self.by_name.get(name) {
            None => HpaAtName::Absent,
            Some(true) => HpaAtName::Owned,
            Some(false) => HpaAtName::Foreign,
        }
    }

    /// The names of the HPAs this RavelCluster owns.
    pub fn owned(&self) -> impl Iterator<Item = &str> {
        self.by_name
            .iter()
            .filter(|(_, owned)| **owned)
            .map(|(name, _)| name.as_str())
    }
}

/// The `Managed` HPA for a tier (ADR-4314 decision 4), or `None` when the
/// tier has no active `Managed` block or is the disabled maintain tier, whose
/// Deployment the operator deletes.
pub fn desired_hpa(
    spec: &RavelClusterSpec,
    instance: &str,
    tier: DeploymentTier,
) -> Option<HorizontalPodAutoscaler> {
    if tier == DeploymentTier::Maintain && !spec.maintain.enabled {
        return None;
    }
    let autoscaling = tier_autoscaling(spec, tier)?;
    if autoscaling.mode != AutoscalingMode::Managed || withheld_reason(spec, tier).is_some() {
        return None;
    }
    let target = autoscaling.target_cpu_utilization_percentage?;
    let name = hpa_name(instance, tier);
    let period_seconds = match tier {
        DeploymentTier::Maintain => MAINTAIN_SCALE_DOWN_PERIOD_SECONDS,
        DeploymentTier::Gateway | DeploymentTier::Query => {
            REQUEST_SERVING_SCALE_DOWN_PERIOD_SECONDS
        }
    };
    Some(HorizontalPodAutoscaler {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            labels: Some(labels(instance, tier.component())),
            ..ObjectMeta::default()
        },
        spec: Some(HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                api_version: Some("apps/v1".to_string()),
                kind: "Deployment".to_string(),
                name,
            },
            min_replicas: Some(autoscaling.min_replicas),
            max_replicas: autoscaling.max_replicas,
            metrics: Some(vec![MetricSpec {
                type_: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        type_: "Utilization".to_string(),
                        average_utilization: Some(target),
                        ..MetricTarget::default()
                    },
                }),
                ..MetricSpec::default()
            }]),
            behavior: Some(HorizontalPodAutoscalerBehavior {
                scale_down: Some(HPAScalingRules {
                    stabilization_window_seconds: autoscaling.scale_down_stabilization_seconds,
                    policies: Some(vec![HPAScalingPolicy {
                        type_: "Pods".to_string(),
                        value: 1,
                        period_seconds,
                    }]),
                    ..HPAScalingRules::default()
                }),
                scale_up: None,
            }),
        }),
        status: None,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
        FieldsV1, ManagedFieldsEntry, OwnerReference,
    };

    const GATEWAY: DeploymentTier = DeploymentTier::Gateway;

    /// A minimal spec with `gateway` (and `maintain`) merged in.
    fn spec_with(gateway: Value, maintain: Value) -> RavelClusterSpec {
        serde_json::from_value(json!({
            "image": "ravel-server:test",
            "shards": 1,
            "storage": { "s3": {
                "bucket": "ravel",
                "region": "us-east-2",
                "credentialsSecretRef": { "name": "ravel-s3" },
            }},
            "gateway": gateway,
            "maintain": maintain,
        }))
        .expect("spec parses")
    }

    fn spec(gateway: Value) -> RavelClusterSpec {
        spec_with(gateway, json!({}))
    }

    fn managed() -> Value {
        json!({ "mode": "Managed", "minReplicas": 2, "maxReplicas": 4,
                "targetCPUUtilizationPercentage": 70 })
    }

    fn external() -> Value {
        json!({ "mode": "External", "minReplicas": 2, "maxReplicas": 4 })
    }

    fn entry(declared: i32) -> AutoscaledTier {
        AutoscaledTier {
            tier: ScalableTier::Gateway,
            declared_replicas: declared,
        }
    }

    fn record(adopted: i32, declared: i32) -> ReplicaAdoption {
        ReplicaAdoption {
            tier: ScalableTier::Gateway,
            adopted_replicas: adopted,
            declared_replicas: declared,
        }
    }

    fn owned(replicas: i32) -> Option<LiveTier> {
        Some(LiveTier {
            replicas: Some(replicas),
            operator_owns_replicas: true,
        })
    }

    fn released(replicas: i32) -> Option<LiveTier> {
        Some(LiveTier {
            replicas: Some(replicas),
            operator_owns_replicas: false,
        })
    }

    #[test]
    fn active_tier_hands_over_the_live_count_unclamped() {
        let spec = spec(json!({ "replicas": 3, "autoscaling": managed() }));
        let plan = plan_tier(&spec, GATEWAY, None, None, owned(7), false).expect("plan");
        assert_eq!(
            plan,
            TierPlan {
                replicas: Replicas::Released { handover: Some(7) },
                render_hpa: true,
                delete_hpa: false,
                autoscaled: Some(entry(3)),
                adoption: None,
                record_first: false,
            }
        );
    }

    #[test]
    fn released_tier_is_not_handed_over_again() {
        let spec = spec(json!({ "replicas": 3, "autoscaling": managed() }));
        let plan =
            plan_tier(&spec, GATEWAY, Some(&entry(3)), None, released(4), false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Released { handover: None });
    }

    #[test]
    fn new_tier_starts_at_min_replicas_and_records_nothing() {
        // ADR step 0: the create pass stops at minReplicas and records the
        // tier only after the handover, so `autoscaled` is None here.
        let spec = spec(json!({ "replicas": 3, "autoscaling": managed() }));
        let plan = plan_tier(&spec, GATEWAY, None, None, None, false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Fixed(2));
        assert!(!plan.render_hpa);
        assert_eq!(plan.autoscaled, None);
    }

    #[test]
    fn owned_live_without_replicas_errors_rather_than_handing_over_one() {
        // The operator owns `spec.replicas` but the live object carries none:
        // a corrupt object. Erroring here is item 5; `unwrap_or(1)` would have
        // handed over 1 and dropped the tier.
        let spec = spec(json!({ "replicas": 3, "autoscaling": managed() }));
        let live = Some(LiveTier {
            replicas: None,
            operator_owns_replicas: true,
        });
        assert_eq!(
            plan_tier(&spec, GATEWAY, None, None, live, false),
            Err(PlanError::HandoverReplicasMissing)
        );
        // A released (not operator-owned) live object with no replicas is fine:
        // there is nothing to hand over.
        let released = Some(LiveTier {
            replicas: None,
            operator_owns_replicas: false,
        });
        assert_eq!(
            plan_tier(&spec, GATEWAY, Some(&entry(3)), None, released, false)
                .expect("plan")
                .replicas,
            Replicas::Released { handover: None }
        );
    }

    #[test]
    fn external_mode_renders_no_hpa_and_sweeps_a_managed_one() {
        let spec = spec(json!({ "replicas": 3, "autoscaling": external() }));
        let plan = plan_tier(&spec, GATEWAY, None, None, owned(3), false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Released { handover: Some(3) });
        assert!(!plan.render_hpa);
        assert!(plan.delete_hpa);
        assert!(desired_hpa(&spec, "rc", GATEWAY).is_none());
    }

    #[test]
    fn turn_off_with_replicas_unchanged_records_the_live_count_first() {
        let spec = spec(json!({ "replicas": 3 }));
        let plan =
            plan_tier(&spec, GATEWAY, Some(&entry(3)), None, released(12), false).expect("plan");
        assert_eq!(
            plan,
            TierPlan {
                replicas: Replicas::Fixed(12),
                render_hpa: false,
                delete_hpa: true,
                autoscaled: None,
                adoption: Some(record(12, 3)),
                record_first: true,
            }
        );
    }

    #[test]
    fn turn_off_with_replicas_changed_applies_the_declared_count() {
        let spec = spec(json!({ "replicas": 5 }));
        let plan =
            plan_tier(&spec, GATEWAY, Some(&entry(3)), None, released(12), false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Fixed(5));
        assert_eq!(plan.adoption, None);
        assert!(!plan.record_first);
        assert!(plan.delete_hpa);
    }

    #[test]
    fn withheld_tier_turns_off_like_a_removed_block() {
        let spec = spec(json!({
            "replicas": 3,
            "resources": { "requests": { "memory": "1Gi" } },
            "autoscaling": managed(),
        }));
        let plan =
            plan_tier(&spec, GATEWAY, Some(&entry(3)), None, released(6), false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Fixed(6));
        assert_eq!(plan.adoption, Some(record(6, 3)));
        assert!(plan.record_first);
        assert!(!plan.render_hpa);
        assert!(plan.delete_hpa);
    }

    #[test]
    fn adoption_record_holds_until_replicas_changes() {
        let unchanged = spec(json!({ "replicas": 3 }));
        let plan =
            plan_tier(&unchanged, GATEWAY, None, Some(&record(12, 3)), None, false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Fixed(12));
        assert_eq!(plan.adoption, Some(record(12, 3)));
        assert!(!plan.record_first);

        let changed = spec(json!({ "replicas": 4 }));
        let plan =
            plan_tier(&changed, GATEWAY, None, Some(&record(12, 3)), None, false).expect("plan");
        assert_eq!(plan.replicas, Replicas::Fixed(4));
        assert_eq!(plan.adoption, None);
    }

    #[test]
    fn untouched_tier_keeps_todays_behaviour() {
        let spec = spec(json!({ "replicas": 3 }));
        assert!(!needs_live(&spec, GATEWAY, None));
        let plan = plan_tier(&spec, GATEWAY, None, None, None, false).expect("plan");
        assert_eq!(
            plan,
            TierPlan {
                replicas: Replicas::Fixed(3),
                render_hpa: false,
                delete_hpa: false,
                autoscaled: None,
                adoption: None,
                record_first: false,
            }
        );
    }

    fn managed_entry(
        manager: &str,
        subresource: Option<&str>,
        fields: Value,
    ) -> ManagedFieldsEntry {
        ManagedFieldsEntry {
            manager: Some(manager.to_string()),
            subresource: subresource.map(str::to_string),
            fields_v1: Some(FieldsV1(fields)),
            ..ManagedFieldsEntry::default()
        }
    }

    fn deployment(replicas: i32, managed: Option<Vec<ManagedFieldsEntry>>) -> Deployment {
        let mut deployment = Deployment::default();
        deployment.metadata.managed_fields = managed;
        deployment.spec = Some(k8s_openapi::api::apps::v1::DeploymentSpec {
            replicas: Some(replicas),
            ..Default::default()
        });
        deployment
    }

    #[test]
    fn live_tier_reads_replicas_ownership_from_managed_fields() {
        let replicas = json!({ "f:spec": { "f:replicas": {} } });
        let template = json!({ "f:spec": { "f:template": {} } });

        let applied = deployment(
            3,
            Some(vec![managed_entry(
                "ravel-operator",
                None,
                replicas.clone(),
            )]),
        );
        assert_eq!(
            LiveTier::of(&applied),
            LiveTier {
                replicas: Some(3),
                operator_owns_replicas: true
            }
        );

        let scaled = deployment(
            5,
            Some(vec![
                managed_entry("ravel-operator", None, template),
                managed_entry("kubectl", Some("scale"), replicas.clone()),
            ]),
        );
        assert!(!LiveTier::of(&scaled).operator_owns_replicas);

        let handed_over = deployment(
            5,
            Some(vec![managed_entry(
                HANDOVER_FIELD_MANAGER,
                None,
                replicas.clone(),
            )]),
        );
        assert!(!LiveTier::of(&handed_over).operator_owns_replicas);

        let status_only = deployment(
            5,
            Some(vec![managed_entry(
                "ravel-operator",
                Some("status"),
                replicas,
            )]),
        );
        assert!(!LiveTier::of(&status_only).operator_owns_replicas);

        assert!(LiveTier::of(&deployment(5, None)).operator_owns_replicas);
    }

    #[test]
    fn managed_needs_a_cpu_request_or_limit() {
        let none = spec(json!({ "resources": { "requests": { "memory": "1Gi" } },
                               "autoscaling": managed() }));
        assert_eq!(
            withheld_reason(&none, GATEWAY),
            Some(WithheldReason::NoCpuRequest)
        );
        let limit = spec(json!({ "resources": { "limits": { "cpu": "1" } },
                                "autoscaling": managed() }));
        assert_eq!(withheld_reason(&limit, GATEWAY), None);
        let default = spec(json!({ "autoscaling": managed() }));
        assert_eq!(withheld_reason(&default, GATEWAY), None);
        let external = spec(json!({ "resources": { "requests": { "memory": "1Gi" } },
                                   "autoscaling": external() }));
        assert_eq!(withheld_reason(&external, GATEWAY), None);
    }

    #[test]
    fn ingress_nginx_affinity_withholds_only_the_gateway_while_enabled() {
        let nginx = spec_with(
            json!({ "autoscaling": external(),
                    "ingestAffinity": { "enabled": true, "backend": "ingressNginx" } }),
            json!({ "autoscaling": external() }),
        );
        assert_eq!(
            withheld_reason(&nginx, GATEWAY),
            Some(WithheldReason::IngressNginxAffinity)
        );
        assert_eq!(withheld_reason(&nginx, DeploymentTier::Maintain), None);

        let disabled = spec(json!({ "autoscaling": external(),
                                    "ingestAffinity": { "enabled": false } }));
        assert_eq!(withheld_reason(&disabled, GATEWAY), None);

        let native = spec(json!({ "autoscaling": external(),
                                  "ingestAffinity": { "enabled": true, "backend": "ravelNative" } }));
        assert_eq!(withheld_reason(&native, GATEWAY), None);
    }

    #[test]
    fn withheld_message_names_each_withheld_tier() {
        let spec = spec_with(
            json!({ "autoscaling": external(),
                    "ingestAffinity": { "enabled": true, "backend": "ingressNginx" } }),
            json!({ "resources": { "requests": { "memory": "1Gi" } },
                    "autoscaling": managed() }),
        );
        let message = withheld_message(&spec, &[]).expect("two tiers withheld");
        assert!(message.contains("gateway: ingestAffinity"), "{message}");
        assert!(
            message.contains("maintain: mode Managed needs a CPU request"),
            "{message}"
        );
        assert!(!message.contains("query:"), "{message}");
        assert_eq!(
            withheld_message(&spec_with(json!({}), json!({})), &[]),
            None
        );
    }

    #[test]
    fn desired_hpa_paces_scale_down_per_tier() {
        let spec = spec_with(
            json!({ "autoscaling": managed() }),
            json!({ "autoscaling": {
                "mode": "Managed", "minReplicas": 1, "maxReplicas": 3,
                "targetCPUUtilizationPercentage": 80, "scaleDownStabilizationSeconds": 120,
            } }),
        );
        let gateway = serde_json::to_value(desired_hpa(&spec, "rc", GATEWAY).expect("gateway HPA"))
            .expect("serialize");
        assert_eq!(
            gateway["spec"],
            json!({
                "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment", "name": "rc-gateway" },
                "minReplicas": 2,
                "maxReplicas": 4,
                "metrics": [{ "type": "Resource", "resource": { "name": "cpu",
                    "target": { "type": "Utilization", "averageUtilization": 70 } } }],
                "behavior": { "scaleDown": {
                    "policies": [{ "type": "Pods", "value": 1, "periodSeconds": 60 }] } },
            })
        );
        assert_eq!(gateway["metadata"]["name"], "rc-gateway");

        let maintain = serde_json::to_value(
            desired_hpa(&spec, "rc", DeploymentTier::Maintain).expect("maintain HPA"),
        )
        .expect("serialize");
        assert_eq!(
            maintain["spec"]["behavior"],
            json!({ "scaleDown": { "stabilizationWindowSeconds": 120,
                "policies": [{ "type": "Pods", "value": 1, "periodSeconds": 600 }] } })
        );
        assert!(desired_hpa(&spec, "rc", DeploymentTier::Query).is_none());
    }

    #[test]
    fn scale_down_periods_exceed_the_drain_bounds() {
        let period = i64::from(REQUEST_SERVING_SCALE_DOWN_PERIOD_SECONDS);
        assert!(period > crate::reconcile::POD_TERMINATION_GRACE_PERIOD_WITH_HEALTH_PORT_SECONDS);
        assert!(period > crate::reconcile::POD_TERMINATION_GRACE_PERIOD_SECONDS);
        // Liveness window, next heartbeat tick, next jittered fold tick
        // (docs/guides/operations/maintenance.md).
        let maintain_handover_bound: i64 = 3 * 60 + 60 + (300 + 30);
        assert_eq!(maintain_handover_bound, 570);
        assert!(i64::from(MAINTAIN_SCALE_DOWN_PERIOD_SECONDS) > maintain_handover_bound);
    }

    #[test]
    fn ownership_needs_the_labels_and_this_clusters_owner_reference() {
        const UID: &str = "uid-rc";
        let hpa = |name: &str, instance: &str, owner_uid: Option<&str>| ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(labels(instance, "gateway")),
            owner_references: owner_uid.map(|uid| {
                vec![OwnerReference {
                    api_version: "ravel.nofire.ai/v1alpha1".to_string(),
                    kind: "RavelCluster".to_string(),
                    name: instance.to_string(),
                    uid: uid.to_string(),
                    ..OwnerReference::default()
                }]
            }),
            ..ObjectMeta::default()
        };
        // What `desired_hpa` renders plus the owner reference the controller
        // stamps.
        assert!(hpa_is_operator_owned(
            &hpa("rc-gateway", "rc", Some(UID)),
            "rc",
            UID
        ));
        // The labels with no owner reference: a copy saved under another name.
        assert!(!hpa_is_operator_owned(&hpa("copy", "rc", None), "rc", UID));
        // The labels with another cluster's uid (a RavelCluster of the same
        // name deleted and created again, with the HPA orphaned).
        assert!(!hpa_is_operator_owned(
            &hpa("rc-gateway", "rc", Some("uid-old")),
            "rc",
            UID
        ));
        // This cluster's owner reference on another instance's labels.
        assert!(!hpa_is_operator_owned(
            &hpa("rc-gateway", "other", Some(UID)),
            "rc",
            UID
        ));
        // No labels at all: `kubectl autoscale`.
        let unlabelled = ObjectMeta {
            labels: None,
            ..hpa("rc-gateway", "rc", Some(UID))
        };
        assert!(!hpa_is_operator_owned(&unlabelled, "rc", UID));
        // The `instance` label without the operator's `managed-by` value.
        let foreign = ObjectMeta {
            labels: Some(BTreeMap::from([(
                INSTANCE_LABEL.to_string(),
                "rc".to_string(),
            )])),
            ..hpa("rc-gateway", "rc", Some(UID))
        };
        assert!(!hpa_is_operator_owned(&foreign, "rc", UID));
    }

    #[test]
    fn hpa_listing_tells_owned_from_foreign_by_name() {
        let owned = ObjectMeta {
            name: Some("rc-gateway".to_string()),
            labels: Some(labels("rc", "gateway")),
            owner_references: Some(vec![OwnerReference {
                uid: "uid-rc".to_string(),
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        };
        let user = ObjectMeta {
            name: Some("rc-query".to_string()),
            ..ObjectMeta::default()
        };
        let listing = HpaListing::new([&owned, &user], "rc", Some("uid-rc"));
        assert_eq!(listing.at("rc-gateway"), HpaAtName::Owned);
        assert_eq!(listing.at("rc-query"), HpaAtName::Foreign);
        assert_eq!(listing.at("rc-maintain"), HpaAtName::Absent);
        assert_eq!(listing.owned().collect::<Vec<_>>(), vec!["rc-gateway"]);
        // No uid for the RavelCluster: nothing is owned.
        let no_uid = HpaListing::new([&owned], "rc", None);
        assert_eq!(no_uid.at("rc-gateway"), HpaAtName::Foreign);
        assert_eq!(no_uid.owned().count(), 0);
    }

    #[test]
    fn a_taken_hpa_name_withholds_only_a_managed_tier() {
        let managed_spec = spec(json!({ "autoscaling": managed() }));
        assert_eq!(
            tier_withheld_reason(&managed_spec, GATEWAY, true),
            Some(WithheldReason::HpaNameTaken)
        );
        assert_eq!(tier_withheld_reason(&managed_spec, GATEWAY, false), None);
        // External renders no HPA, so a user HPA at the name is its normal case.
        let external_spec = spec(json!({ "autoscaling": external() }));
        assert_eq!(tier_withheld_reason(&external_spec, GATEWAY, true), None);
        // A spec reason wins over the taken name.
        let no_cpu = spec(json!({
            "resources": { "requests": { "memory": "1Gi" } },
            "autoscaling": managed(),
        }));
        assert_eq!(
            tier_withheld_reason(&no_cpu, GATEWAY, true),
            Some(WithheldReason::NoCpuRequest)
        );

        let plan = plan_tier(&managed_spec, GATEWAY, None, None, owned(3), true).expect("plan");
        assert_eq!(
            plan.replicas,
            Replicas::Fixed(tier_replicas(&managed_spec, GATEWAY))
        );
        assert!(!plan.render_hpa);
        assert_eq!(plan.autoscaled, None);

        let message = withheld_message(&managed_spec, &[GATEWAY]).expect("withheld");
        assert!(
            message.contains("gateway: mode Managed renders its HorizontalPodAutoscaler"),
            "{message}"
        );
        assert_eq!(withheld_message(&managed_spec, &[]), None);
    }

    #[test]
    fn disabled_maintain_desires_no_hpa() {
        let maintain = json!({ "enabled": false, "autoscaling": managed() });
        let disabled = spec_with(json!({}), maintain);
        assert!(desired_hpa(&disabled, "rc", DeploymentTier::Maintain).is_none());
        let enabled = spec_with(json!({}), json!({ "autoscaling": managed() }));
        assert!(desired_hpa(&enabled, "rc", DeploymentTier::Maintain).is_some());
    }

    #[test]
    fn handover_patch_holds_only_identity_and_replicas() {
        assert_eq!(
            handover_patch("rc-gateway", "ns", 7),
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": { "name": "rc-gateway", "namespace": "ns" },
                "spec": { "replicas": 7 },
            })
        );
    }

    #[test]
    fn state_replaces_per_tier_and_clears_to_null() {
        let mut state = AutoscalingState::default();
        state.set(DeploymentTier::Maintain, None, None);
        assert_eq!(
            state.adoptions_patch(),
            json!({ "status": { "replicaAdoptions": null } })
        );
        state.set(
            DeploymentTier::Maintain,
            Some(AutoscaledTier {
                tier: ScalableTier::Maintain,
                declared_replicas: 1,
            }),
            None,
        );
        state.set(GATEWAY, Some(entry(3)), Some(record(9, 3)));
        assert_eq!(
            state.autoscaled_tiers().expect("two entries")[0].tier,
            ScalableTier::Gateway
        );
        state.set(GATEWAY, None, Some(record(9, 3)));
        assert_eq!(
            state.adoptions_patch(),
            json!({ "status": { "replicaAdoptions": [
                { "tier": "gateway", "adoptedReplicas": 9, "declaredReplicas": 3 }
            ] } })
        );
        assert_eq!(state.autoscaled_tiers().map(|t| t.len()), Some(1));
        assert!(state.entry(GATEWAY).is_none());
        assert_eq!(state.adoption(GATEWAY), Some(&record(9, 3)));
    }
}
