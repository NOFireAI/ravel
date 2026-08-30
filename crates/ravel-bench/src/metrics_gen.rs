//! The deterministic MetricsBench workload generator (ADR-0927 decision 11).
//!
//! Given a gated [`WorkloadFile`] and a profile name, this produces the sample
//! stream the portable lane ingests over Remote Write 1.0: gauges, monotonic
//! counters, classic histograms, native histograms, staleness markers, counter
//! resets, missing samples, out-of-order samples, and hour-boundary churn.
//!
//! Determinism is structural, not a convention. Every value and every injected
//! anomaly is a pure function of `(seed, family, instance, step)` through
//! [`splitmix64`]: there is no RNG state carried between samples, no clock
//! read, and no iteration over a hash map. Two runs of the same seed and
//! manifest therefore write byte-identical output, which
//! `same_seed_produces_byte_identical_output` proves rather than asserting in
//! prose.
//!
//! Float values are built as an integer over a power of two
//! ([`unit_scaled`]), so a value is exactly representable and reproduces bit
//! for bit; the encoder writes float payloads as their `f64::to_bits` hex, so
//! a NaN or a `-0.0` is distinguishable in the stream rather than collapsed by
//! decimal formatting.
//!
//! ## Anomaly precedence
//!
//! An instance-step can be selected by more than one anomaly, so the order is
//! fixed rather than left to whichever check runs first:
//!
//! 1. **missing** wins outright: the instance emits nothing for that step, and
//!    every series it would have written counts as omitted.
//! 2. **staleness** replaces a gauge's value with a marker. Counters and
//!    histograms are left alone so a reset stays unambiguous.
//! 3. **out-of-order** shifts the timestamp two scrape intervals earlier,
//!    independently of what value was chosen, and only from step 2 on so the
//!    shifted sample really does follow a newer one in the stream.
//! 4. **counter reset** zeroes a counter or histogram instance before that
//!    step's increment, independently of the three above.

use std::collections::BTreeMap;
use std::io::Write;

use serde::Serialize;

use crate::metrics_workload::{
    CHURN_EPOCH_SECS, FamilyKind, MetricFamily, Profile, WorkloadFile,
};

/// Hash-domain tags, so two derivations from the same `(family, instance,
/// step)` triple never collide.
const TAG_VALUE: u64 = 0x01;
const TAG_MISSING: u64 = 0x11;
const TAG_STALE: u64 = 0x12;
const TAG_RESET: u64 = 0x13;
const TAG_OUT_OF_ORDER: u64 = 0x14;

/// SplitMix64's finalizer, the whole source of variation in this generator: a
/// pure function, so nothing depends on iteration or call order.
pub fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Mixes a seed and a coordinate tuple into one value.
pub fn mix(seed: u64, coords: &[u64]) -> u64 {
    let mut h = splitmix64(seed);
    for c in coords {
        h = splitmix64(h ^ splitmix64(*c));
    }
    h
}

/// `h` reduced to `[0, range)` over a power-of-two denominator, so the result
/// is an exactly representable `f64` and reproduces bit for bit.
pub fn unit_scaled(h: u64, range_numerator: u64, denominator_pow2: u32) -> f64 {
    let denom = 1u64 << denominator_pow2;
    (h % (range_numerator * denom)) as f64 / denom as f64
}

/// What one series carries at one timestamp.
#[derive(Clone, Debug, PartialEq)]
pub enum SampleValue {
    /// A float sample.
    Float(f64),
    /// A Prometheus staleness marker: the series is explicitly absent from this
    /// scrape rather than merely missing from it.
    StaleMarker,
    /// A native histogram (ADR-0108).
    NativeHistogram(NativeHistogram),
}

/// A native histogram sample.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeHistogram {
    /// The bucket-layout schema.
    pub schema: i32,
    /// Total observations.
    pub count: u64,
    /// Sum of observations.
    pub sum: f64,
    /// Positive-bucket counts as deltas, Prometheus' own wire shape.
    pub positive_deltas: Vec<i64>,
}

/// One generated sample: a series identity, a timestamp, and a value.
#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedSample {
    /// Milliseconds since the epoch.
    pub ts_ms: i64,
    /// The metric name, including any classic-histogram suffix.
    pub metric: String,
    /// The labels, sorted by name.
    pub labels: Vec<(String, String)>,
    /// The value.
    pub value: SampleValue,
}

impl GeneratedSample {
    /// The canonical one-line encoding: `<ts_ms>\t<series>\t<payload>\n`.
    ///
    /// Floats are written as `f64::to_bits` hex rather than decimal so the
    /// stream distinguishes `-0.0` from `0.0` and carries a NaN payload
    /// unchanged.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.clear();
        let mut line = format!("{}\t{}{{", self.ts_ms, self.metric);
        for (i, (k, v)) in self.labels.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            line.push_str(k);
            line.push_str("=\"");
            line.push_str(v);
            line.push('"');
        }
        line.push_str("}\t");
        match &self.value {
            SampleValue::Float(v) => {
                line.push_str(&format!("f 0x{:016x}", v.to_bits()));
            }
            SampleValue::StaleMarker => line.push_str("stale"),
            SampleValue::NativeHistogram(h) => {
                line.push_str(&format!(
                    "h schema={} count={} sum=0x{:016x} deltas=",
                    h.schema,
                    h.count,
                    h.sum.to_bits()
                ));
                for (i, d) in h.positive_deltas.iter().enumerate() {
                    if i > 0 {
                        line.push(',');
                    }
                    line.push_str(&d.to_string());
                }
            }
        }
        line.push('\n');
        out.extend_from_slice(line.as_bytes());
    }
}

/// Everything a generation run reports. Every figure is exact: a reader gates
/// on these rather than on the absence of a complaint (ADR-0927's band 4:
/// ingested must equal generated minus explicitly reported rejections).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GenerationReport {
    /// The profile generated.
    pub profile: String,
    /// The seed the manifest declared.
    pub seed: u64,
    /// Whether this profile's figures may be published (ADR-0927 decision 11).
    pub publishable: bool,
    /// Why not, when they may not.
    pub non_comparable_reason: Option<String>,
    /// Steps generated. Equals the profile's `samples_per_series` for a full
    /// run and less for a bounded one.
    pub steps: u64,
    /// Series alive at any one step.
    pub active_series: u64,
    /// Distinct series created across the run, including churned-in cohorts.
    pub total_series_created: u64,
    /// `steps * active_series`: samples a run with no anomalies would write.
    pub nominal_samples: u64,
    /// Samples actually written.
    pub emitted_samples: u64,
    /// Float samples among them.
    pub float_samples: u64,
    /// Staleness markers among them.
    pub stale_markers: u64,
    /// Native-histogram samples among them.
    pub native_histogram_samples: u64,
    /// Samples not written because their scrape was dropped. Plus
    /// `emitted_samples`, this equals `nominal_samples`.
    pub omitted_missing_samples: u64,
    /// Counter and histogram instances reset to zero mid-run.
    pub counter_reset_events: u64,
    /// Samples stamped two intervals early.
    pub out_of_order_samples: u64,
    /// Bytes written.
    pub bytes: u64,
    /// BLAKE3 of the byte stream, so two runs are compared by one field.
    pub digest: String,
}

/// Everything that can go wrong generating a workload.
#[derive(Debug, thiserror::Error)]
pub enum GeneratorError {
    /// The manifest declares no profile by that name.
    #[error("workload manifest declares no profile `{name}`; it declares {available:?}")]
    UnknownProfile {
        /// The name asked for.
        name: String,
        /// The names the manifest declares.
        available: Vec<String>,
    },
    /// Writing the stream failed.
    #[error("writing the generated stream failed: {source}")]
    Write {
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The run's own accounting does not close. A silent drop is exactly what
    /// ADR-0927's band 4 fails a run for, so it is a hard error here rather
    /// than a figure a reader has to notice.
    #[error(
        "generated sample accounting does not close for profile `{profile}`: {emitted} emitted \
         plus {omitted} omitted is {sum}, but {steps} steps over {active_series} active series \
         is {nominal}"
    )]
    AccountingMismatch {
        /// The profile generated.
        profile: String,
        /// Samples written.
        emitted: u64,
        /// Samples omitted.
        omitted: u64,
        /// Their sum.
        sum: u64,
        /// Steps generated.
        steps: u64,
        /// Series alive per step.
        active_series: u64,
        /// The nominal total.
        nominal: u64,
    },
}

/// Per-instance accumulated state for the monotonic families.
#[derive(Clone, Debug)]
enum InstanceState {
    /// A counter's running total, in whole units.
    Counter { total: u64 },
    /// A classic histogram's cumulative per-bucket counts (the last entry is
    /// `+Inf`), observation sum, and observation count.
    Classic {
        buckets: Vec<u64>,
        sum: f64,
        count: u64,
    },
}

/// One family's generation plan under one profile.
#[derive(Debug)]
struct FamilyPlan {
    /// Index into [`WorkloadFile::families`].
    index: usize,
    /// Instances alive at any one step.
    instances: u64,
    /// Series one instance emits.
    series_per_instance: u64,
    /// Instances replaced at each churn-epoch boundary.
    churned_per_epoch: u64,
    /// `(cardinality, stride)` for each fixed label, in declaration order.
    fixed: Vec<(u64, u64)>,
    /// Product of the fixed cardinalities: the instance ordinal's divisor.
    fixed_product: u64,
    /// Accumulated state, keyed by global instance index. Pruned at each epoch
    /// boundary so a long churn run does not grow without bound.
    state: BTreeMap<u64, InstanceState>,
}

/// The deterministic generator.
#[derive(Debug)]
pub struct Generator<'a> {
    workload: &'a WorkloadFile,
    profile: &'a Profile,
    base_ts_ms: i64,
    plans: Vec<FamilyPlan>,
}

impl<'a> Generator<'a> {
    /// Build a generator for `profile_name`, with the first step stamped at
    /// `base_ts_ms`. Time is a parameter, never a clock read, so a run is
    /// reproducible.
    pub fn new(
        workload: &'a WorkloadFile,
        profile_name: &str,
        base_ts_ms: i64,
    ) -> Result<Self, GeneratorError> {
        let profile = workload.profile(profile_name).ok_or_else(|| {
            GeneratorError::UnknownProfile {
                name: profile_name.to_string(),
                available: workload.profiles.iter().map(|p| p.name.clone()).collect(),
            }
        })?;
        let mut plans = Vec::with_capacity(workload.families.len());
        for (index, family) in workload.families.iter().enumerate() {
            let instances = workload.family_instances(profile, family);
            let mut fixed = Vec::with_capacity(family.labels.len());
            let mut stride = 1u64;
            for label in &family.labels {
                // `gate_workload` already refused a family naming an
                // undeclared dimension, so an absent one here would be a
                // manifest that never passed the gate; treat it as a
                // single-valued dimension rather than panicking.
                let card = workload
                    .dimension(label)
                    .map(|d| d.values.len() as u64)
                    .unwrap_or(1)
                    .max(1);
                fixed.push((card, stride));
                stride *= card;
            }
            plans.push(FamilyPlan {
                index,
                instances,
                series_per_instance: workload.series_per_instance(family.kind),
                churned_per_epoch: instances * profile.churn_basis_points_per_hour / 10_000,
                fixed,
                fixed_product: stride,
                state: BTreeMap::new(),
            });
        }
        Ok(Generator {
            workload,
            profile,
            base_ts_ms,
            plans,
        })
    }

    /// The profile this generator runs.
    pub fn profile(&self) -> &Profile {
        self.profile
    }

    /// Distinct series the plans create over `steps`, counting churned-in
    /// cohorts. Computed from the plans rather than from the profile's own
    /// arithmetic, so a disagreement between the two is visible.
    pub fn total_series_created(&self, steps: u64) -> u64 {
        let epochs = self.epochs_spanned(steps);
        self.plans
            .iter()
            .map(|p| (p.instances + (epochs - 1) * p.churned_per_epoch) * p.series_per_instance)
            .sum()
    }

    /// Churn epochs `steps` spans, at least one.
    fn epochs_spanned(&self, steps: u64) -> u64 {
        if steps == 0 {
            return 1;
        }
        let last_secs = (steps - 1) * self.profile.scrape_interval_secs;
        last_secs / CHURN_EPOCH_SECS + 1
    }

    /// Generate `steps` scrapes into `sink`, returning the run's exact figures.
    pub fn generate_into<W: Write>(
        &mut self,
        steps: u64,
        sink: &mut W,
    ) -> Result<GenerationReport, GeneratorError> {
        let interval_ms = (self.profile.scrape_interval_secs * 1_000) as i64;
        let seed = self.workload.seed;
        let anomalies = self.workload.generator.anomalies;
        let mut hasher = blake3::Hasher::new();
        let mut line = Vec::with_capacity(256);
        let mut samples: Vec<GeneratedSample> = Vec::new();

        let mut emitted = 0u64;
        let mut floats = 0u64;
        let mut stale = 0u64;
        let mut native = 0u64;
        let mut omitted = 0u64;
        let mut resets = 0u64;
        let mut out_of_order = 0u64;
        let mut bytes = 0u64;

        for step in 0..steps {
            let step_secs = step * self.profile.scrape_interval_secs;
            let epoch = step_secs / CHURN_EPOCH_SECS;
            let step_ts = self.base_ts_ms + (step as i64) * interval_ms;

            for plan_idx in 0..self.plans.len() {
                let family = self.workload.families[self.plans[plan_idx].index].clone();
                let base = self.plans[plan_idx].churned_per_epoch * epoch;
                if base > 0 {
                    self.plans[plan_idx].state.retain(|k, _| *k >= base);
                }
                let instances = self.plans[plan_idx].instances;
                for local in 0..instances {
                    let global = base + local;
                    let fam_id = self.plans[plan_idx].index as u64;
                    let per_instance = self.plans[plan_idx].series_per_instance;

                    if fires(
                        anomalies.missing_sample_one_in,
                        seed,
                        TAG_MISSING,
                        fam_id,
                        global,
                        step,
                    ) {
                        omitted += per_instance;
                        continue;
                    }

                    let mut ts = step_ts;
                    let shifted = step >= 2
                        && fires(
                            anomalies.out_of_order_one_in,
                            seed,
                            TAG_OUT_OF_ORDER,
                            fam_id,
                            global,
                            step,
                        );
                    if shifted {
                        ts -= 2 * interval_ms;
                    }

                    let reset = matches!(
                        family.kind,
                        FamilyKind::Counter | FamilyKind::ClassicHistogram
                    ) && fires(
                        anomalies.counter_reset_one_in,
                        seed,
                        TAG_RESET,
                        fam_id,
                        global,
                        step,
                    );
                    if reset {
                        self.plans[plan_idx].state.remove(&global);
                        resets += 1;
                    }

                    let stale_now = family.kind == FamilyKind::Gauge
                        && fires(
                            anomalies.stale_marker_one_in,
                            seed,
                            TAG_STALE,
                            fam_id,
                            global,
                            step,
                        );

                    samples.clear();
                    let labels = self.labels_for(plan_idx, &family, global);
                    self.emit_instance(
                        plan_idx, &family, global, step, ts, &labels, stale_now, &mut samples,
                    );

                    if shifted {
                        out_of_order += samples.len() as u64;
                    }
                    for sample in &samples {
                        match &sample.value {
                            SampleValue::Float(_) => floats += 1,
                            SampleValue::StaleMarker => stale += 1,
                            SampleValue::NativeHistogram(_) => native += 1,
                        }
                        sample.encode(&mut line);
                        hasher.update(&line);
                        sink.write_all(&line)
                            .map_err(|source| GeneratorError::Write { source })?;
                        bytes += line.len() as u64;
                        emitted += 1;
                    }
                }
            }
        }
        sink.flush()
            .map_err(|source| GeneratorError::Write { source })?;

        let nominal = steps * self.profile.active_series;
        if emitted + omitted != nominal {
            return Err(GeneratorError::AccountingMismatch {
                profile: self.profile.name.clone(),
                emitted,
                omitted,
                sum: emitted + omitted,
                steps,
                active_series: self.profile.active_series,
                nominal,
            });
        }

        Ok(GenerationReport {
            profile: self.profile.name.clone(),
            seed,
            publishable: self.profile.is_publishable(),
            non_comparable_reason: self
                .profile
                .comparability
                .reason()
                .map(|r| r.to_string()),
            steps,
            active_series: self.profile.active_series,
            total_series_created: self.total_series_created(steps),
            nominal_samples: nominal,
            emitted_samples: emitted,
            float_samples: floats,
            stale_markers: stale,
            native_histogram_samples: native,
            omitted_missing_samples: omitted,
            counter_reset_events: resets,
            out_of_order_samples: out_of_order,
            bytes,
            digest: hasher.finalize().to_hex().to_string(),
        })
    }

    /// Generate `steps` scrapes into memory. Convenience over
    /// [`Self::generate_into`], through the same single encoding path.
    pub fn generate_bytes(
        &mut self,
        steps: u64,
    ) -> Result<(Vec<u8>, GenerationReport), GeneratorError> {
        let mut buf: Vec<u8> = Vec::new();
        let report = self.generate_into(steps, &mut buf)?;
        Ok((buf, report))
    }

    /// The label set for one instance: its fixed dimensions decomposed from the
    /// global instance index, plus the scaling label carrying the ordinal.
    /// Sorted by name, so a series identity has one spelling.
    fn labels_for(
        &self,
        plan_idx: usize,
        family: &MetricFamily,
        global: u64,
    ) -> Vec<(String, String)> {
        let plan = &self.plans[plan_idx];
        let mut labels: Vec<(String, String)> = Vec::with_capacity(family.labels.len() + 1);
        for (i, label) in family.labels.iter().enumerate() {
            let (card, stride) = plan.fixed[i];
            let value = self
                .workload
                .dimension(label)
                .and_then(|d| d.values.get(((global / stride) % card) as usize).cloned())
                .unwrap_or_default();
            labels.push((label.clone(), value));
        }
        labels.push((
            self.workload.generator.scaling_label.clone(),
            format!(
                "{}{}",
                self.workload.generator.scaling_label_value_prefix,
                global / plan.fixed_product
            ),
        ));
        labels.sort();
        labels
    }

    /// Append every sample one instance writes at one step.
    fn emit_instance(
        &mut self,
        plan_idx: usize,
        family: &MetricFamily,
        global: u64,
        step: u64,
        ts_ms: i64,
        labels: &[(String, String)],
        stale_now: bool,
        out: &mut Vec<GeneratedSample>,
    ) {
        let seed = self.workload.seed;
        let fam_id = self.plans[plan_idx].index as u64;
        let h = mix(seed, &[TAG_VALUE, fam_id, global, step]);
        match family.kind {
            FamilyKind::Gauge => {
                let value = if stale_now {
                    SampleValue::StaleMarker
                } else {
                    SampleValue::Float(unit_scaled(h, 100, 10))
                };
                out.push(GeneratedSample {
                    ts_ms,
                    metric: family.name.clone(),
                    labels: labels.to_vec(),
                    value,
                });
            }
            FamilyKind::Counter => {
                let entry = self.plans[plan_idx]
                    .state
                    .entry(global)
                    .or_insert(InstanceState::Counter { total: 0 });
                let total = match entry {
                    InstanceState::Counter { total } => {
                        *total += h % 16;
                        *total
                    }
                    // A family's kind never changes mid-run, so this arm is
                    // unreachable in practice; restarting the accumulator is
                    // the recoverable answer either way.
                    InstanceState::Classic { .. } => {
                        *entry = InstanceState::Counter { total: h % 16 };
                        h % 16
                    }
                };
                out.push(GeneratedSample {
                    ts_ms,
                    metric: family.name.clone(),
                    labels: labels.to_vec(),
                    value: SampleValue::Float(total as f64),
                });
            }
            FamilyKind::ClassicHistogram => {
                let bounds = self.workload.generator.classic_histogram_bounds.clone();
                let bucket_count = bounds.len() + 1;
                let entry =
                    self.plans[plan_idx]
                        .state
                        .entry(global)
                        .or_insert(InstanceState::Classic {
                            buckets: vec![0; bucket_count],
                            sum: 0.0,
                            count: 0,
                        });
                if !matches!(entry, InstanceState::Classic { .. }) {
                    *entry = InstanceState::Classic {
                        buckets: vec![0; bucket_count],
                        sum: 0.0,
                        count: 0,
                    };
                }
                let (buckets, sum, count) = match entry {
                    InstanceState::Classic {
                        buckets,
                        sum,
                        count,
                    } => {
                        for (b, bucket) in buckets.iter_mut().enumerate() {
                            let observed =
                                mix(seed, &[TAG_VALUE, fam_id, global, step, b as u64]) % 8;
                            *bucket += observed;
                            *count += observed;
                            *sum += unit_scaled(
                                mix(seed, &[TAG_VALUE + 1, fam_id, global, step, b as u64]),
                                1,
                                12,
                            ) * observed as f64;
                        }
                        (buckets.clone(), *sum, *count)
                    }
                    InstanceState::Counter { .. } => (vec![0; bucket_count], 0.0, 0),
                };
                // Cumulative counts, ascending by `le`, then `_sum`, then
                // `_count`: the order a Prometheus scrape presents them in.
                let mut cumulative = 0u64;
                for (b, bucket) in buckets.iter().enumerate() {
                    cumulative += *bucket;
                    let le = match bounds.get(b) {
                        Some(bound) => format!("{bound}"),
                        None => "+Inf".to_string(),
                    };
                    let mut bucket_labels = labels.to_vec();
                    bucket_labels.push(("le".to_string(), le));
                    bucket_labels.sort();
                    out.push(GeneratedSample {
                        ts_ms,
                        metric: format!("{}_bucket", family.name),
                        labels: bucket_labels,
                        value: SampleValue::Float(cumulative as f64),
                    });
                }
                out.push(GeneratedSample {
                    ts_ms,
                    metric: format!("{}_sum", family.name),
                    labels: labels.to_vec(),
                    value: SampleValue::Float(sum),
                });
                out.push(GeneratedSample {
                    ts_ms,
                    metric: format!("{}_count", family.name),
                    labels: labels.to_vec(),
                    value: SampleValue::Float(count as f64),
                });
            }
            FamilyKind::NativeHistogram => {
                let buckets = self.workload.generator.native_histogram_buckets;
                let mut positive_deltas = Vec::with_capacity(buckets as usize);
                let mut previous = 0i64;
                let mut count = 0u64;
                for b in 0..buckets {
                    let absolute =
                        (mix(seed, &[TAG_VALUE, fam_id, global, step, b as u64]) % 32) as i64;
                    positive_deltas.push(absolute - previous);
                    previous = absolute;
                    count += absolute as u64;
                }
                out.push(GeneratedSample {
                    ts_ms,
                    metric: family.name.clone(),
                    labels: labels.to_vec(),
                    value: SampleValue::NativeHistogram(NativeHistogram {
                        schema: self.workload.generator.native_histogram_schema,
                        count,
                        sum: unit_scaled(h, 4, 12),
                        positive_deltas,
                    }),
                });
            }
        }
    }
}

/// Whether an anomaly with rate "one in `one_in`" fires for this coordinate. A
/// rate of zero disables the anomaly, which is how a profile ADR-0927 marks
/// churn-free stays clean.
fn fires(one_in: u64, seed: u64, tag: u64, family: u64, instance: u64, step: u64) -> bool {
    one_in != 0 && mix(seed, &[tag, family, instance, step]) % one_in == 0
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::metrics_workload::{
        AnomalyRates, Comparability, GeneratorConfig, LabelDimension, WORKLOAD_FORMAT_VERSION,
        gate_workload,
    };

    /// A four-family manifest at the `ci` scale, small enough that every figure
    /// below is a number a reader can check by hand: 20 active series, 10 of
    /// them the ten series of one classic-histogram instance.
    fn workload(seed: u64, anomalies: AnomalyRates) -> WorkloadFile {
        let profile = |name: &str, churn_bp: u64, comparability: Comparability| Profile {
            name: name.to_string(),
            active_series: 20,
            samples_per_series: 240,
            scrape_interval_secs: 15,
            duration_secs: 3_600,
            churn_basis_points_per_hour: churn_bp,
            total_samples: 4_800,
            comparability,
        };
        WorkloadFile {
            version: WORKLOAD_FORMAT_VERSION,
            seed,
            generator: GeneratorConfig {
                scaling_label: "instance".to_string(),
                scaling_label_value_prefix: "mb-instance-".to_string(),
                classic_histogram_bounds: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
                native_histogram_schema: 2,
                native_histogram_buckets: 4,
                absent_metric_name: "mb_absent".to_string(),
                anomalies,
            },
            label_dimensions: vec![
                LabelDimension {
                    name: "job".to_string(),
                    values: vec!["api".to_string(), "web".to_string()],
                },
                LabelDimension {
                    name: "region".to_string(),
                    values: vec!["eu".to_string(), "us".to_string()],
                },
            ],
            families: vec![
                MetricFamily {
                    name: "mb_gauge".to_string(),
                    kind: FamilyKind::Gauge,
                    labels: vec!["job".to_string(), "region".to_string()],
                    series_permille: 300,
                },
                MetricFamily {
                    name: "mb_counter".to_string(),
                    kind: FamilyKind::Counter,
                    labels: vec!["job".to_string()],
                    series_permille: 150,
                },
                MetricFamily {
                    name: "mb_classic".to_string(),
                    kind: FamilyKind::ClassicHistogram,
                    labels: vec!["job".to_string()],
                    series_permille: 500,
                },
                MetricFamily {
                    name: "mb_native".to_string(),
                    kind: FamilyKind::NativeHistogram,
                    labels: vec!["job".to_string()],
                    series_permille: 50,
                },
            ],
            profiles: vec![
                profile("cardinality", 0, Comparability::Comparable),
                profile("history", 0, Comparability::Comparable),
                profile("churn", 2_500, Comparability::Comparable),
                profile(
                    "ci",
                    0,
                    Comparability::NonComparable {
                        reason: "too small to measure".to_string(),
                    },
                ),
            ],
        }
    }

    fn clean() -> AnomalyRates {
        AnomalyRates {
            missing_sample_one_in: 0,
            stale_marker_one_in: 0,
            counter_reset_one_in: 0,
            out_of_order_one_in: 0,
        }
    }

    /// The generator's own definition of determinism: the same seed and
    /// manifest produce byte-identical output, and a different seed does not.
    /// Proven on a stream that carries every value shape and every anomaly, so
    /// a nondeterministic anomaly path cannot hide behind a clean run.
    #[test]
    fn same_seed_produces_byte_identical_output() {
        let noisy = AnomalyRates {
            missing_sample_one_in: 7,
            stale_marker_one_in: 5,
            counter_reset_one_in: 6,
            out_of_order_one_in: 4,
        };
        let w = workload(0xABCD_1234, noisy);
        gate_workload(&w).expect("test manifest gates clean");

        let (first_bytes, first) = Generator::new(&w, "churn", 1_700_000_000_000)
            .expect("generator builds")
            .generate_bytes(200)
            .expect("first run generates");
        let (second_bytes, second) = Generator::new(&w, "churn", 1_700_000_000_000)
            .expect("generator builds")
            .generate_bytes(200)
            .expect("second run generates");

        assert_eq!(
            first_bytes, second_bytes,
            "the same seed must produce byte-identical output"
        );
        assert_eq!(first, second, "the same seed must produce identical figures");
        // The run really exercised every shape and every anomaly, so the
        // equality above is not equality of two empty streams.
        assert!(first.bytes > 0, "the run wrote nothing");
        assert_eq!(first.emitted_samples + first.omitted_missing_samples, 4_000);
        for (what, n) in [
            ("float samples", first.float_samples),
            ("stale markers", first.stale_markers),
            ("native histograms", first.native_histogram_samples),
            ("omitted samples", first.omitted_missing_samples),
            ("counter resets", first.counter_reset_events),
            ("out-of-order samples", first.out_of_order_samples),
        ] {
            assert!(n > 0, "the determinism run produced no {what}");
        }

        // A different seed produces a different stream, so the equality above
        // is a property of the seed rather than of a generator that ignores it.
        let other = workload(0xABCD_1235, noisy);
        let (other_bytes, other_report) = Generator::new(&other, "churn", 1_700_000_000_000)
            .expect("generator builds")
            .generate_bytes(200)
            .expect("third run generates");
        assert_ne!(
            other_bytes, first_bytes,
            "a different seed must produce a different stream"
        );
        assert_ne!(other_report.digest, first.digest);
        // The accounting is seed-independent even though the bytes are not.
        assert_eq!(other_report.nominal_samples, first.nominal_samples);
    }

    #[test]
    fn a_clean_run_emits_exactly_one_sample_per_series_per_step() {
        let w = workload(11, clean());
        gate_workload(&w).expect("manifest gates clean");
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(10)
            .expect("generates");

        // 20 active series over 10 steps, with no anomalies enabled.
        assert_eq!(report.nominal_samples, 200);
        assert_eq!(report.emitted_samples, 200);
        assert_eq!(report.omitted_missing_samples, 0);
        assert_eq!(report.stale_markers, 0);
        assert_eq!(report.counter_reset_events, 0);
        assert_eq!(report.out_of_order_samples, 0);
        // 6 gauge + 3 counter + 10 classic + 1 native series per step.
        assert_eq!(report.float_samples, 190);
        assert_eq!(report.native_histogram_samples, 10);
        assert_eq!(report.total_series_created, 20);
        assert_eq!(
            bytes.iter().filter(|b| **b == b'\n').count(),
            200,
            "one line per emitted sample"
        );
        assert_eq!(report.bytes, bytes.len() as u64);
    }

    #[test]
    fn the_series_set_is_exactly_the_declared_cardinality() {
        let w = workload(11, clean());
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(4)
            .expect("generates");
        let text = String::from_utf8(bytes).expect("utf8");
        let series: std::collections::BTreeSet<&str> = text
            .lines()
            .filter_map(|l| l.split('\t').nth(1))
            .collect();
        assert_eq!(
            series.len(),
            report.active_series as usize,
            "distinct series must equal the profile's active-series count"
        );
        // The 6 gauge series are exactly job x region x instance over
        // 2 x 2 x 2 (6 series is 1.5 instance-blocks of the 4 fixed combos, so
        // instances 0..5 span instance ordinals 0 and 1).
        let gauge: std::collections::BTreeSet<&str> = series
            .iter()
            .filter(|s| s.starts_with("mb_gauge{"))
            .copied()
            .collect();
        assert_eq!(gauge.len(), 6);
        assert!(gauge.contains("mb_gauge{instance=\"mb-instance-0\",job=\"api\",region=\"eu\"}"));
        assert!(gauge.contains("mb_gauge{instance=\"mb-instance-1\",job=\"web\",region=\"eu\"}"));
        // Labels are sorted by name in the rendered identity, so a series has
        // exactly one spelling.
        for s in &gauge {
            assert!(
                s.contains("{instance="),
                "labels must be sorted by name: {s}"
            );
        }
    }

    #[test]
    fn counters_are_monotonic_between_resets_and_reset_exactly_when_selected() {
        let w = workload(
            99,
            AnomalyRates {
                missing_sample_one_in: 0,
                stale_marker_one_in: 0,
                counter_reset_one_in: 9,
                out_of_order_one_in: 0,
            },
        );
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(60)
            .expect("generates");
        let text = String::from_utf8(bytes).expect("utf8");

        // Per-series value history for one counter series, in stream order.
        let mut drops = 0usize;
        let mut last: BTreeMap<String, f64> = BTreeMap::new();
        for line in text.lines() {
            let mut parts = line.split('\t');
            let _ts = parts.next();
            let series = parts.next().unwrap_or_default();
            let payload = parts.next().unwrap_or_default();
            if !series.starts_with("mb_counter{") {
                continue;
            }
            let bits = payload
                .strip_prefix("f 0x")
                .and_then(|h| u64::from_str_radix(h, 16).ok())
                .expect("counter payload is hex float bits");
            let value = f64::from_bits(bits);
            if let Some(previous) = last.insert(series.to_string(), value)
                && value < previous
            {
                drops += 1;
            }
        }
        // Resets fire on the counter and the classic-histogram families, and a
        // reset is observable as a drop only when the post-reset increment is
        // smaller than the pre-reset total, so the counter-only observed-drop
        // count is a strict subset of the reported reset count. Both figures are
        // pinned: a generator that stopped resetting, or one that reset without
        // reporting it, moves one of them.
        assert_eq!(report.counter_reset_events, 20);
        assert_eq!(
            drops, 14,
            "counter series must fall only at the resets the report counts"
        );
        assert!(
            (drops as u64) <= report.counter_reset_events,
            "a counter cannot drop more often than it is reset"
        );
    }

    #[test]
    fn classic_histogram_buckets_are_cumulative_and_carry_every_bound() {
        let w = workload(3, clean());
        let (bytes, _) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(3)
            .expect("generates");
        let text = String::from_utf8(bytes).expect("utf8");

        let bucket_lines: Vec<&str> = text
            .lines()
            .filter(|l| l.contains("mb_classic_bucket{"))
            .collect();
        // 8 buckets (7 bounds plus +Inf) for the one instance, over 3 steps.
        assert_eq!(bucket_lines.len(), 24);
        for le in ["0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "+Inf"] {
            assert_eq!(
                bucket_lines
                    .iter()
                    .filter(|l| l.contains(&format!("le=\"{le}\"")))
                    .count(),
                3,
                "every step must carry the le={le} bucket exactly once"
            );
        }
        assert_eq!(
            text.lines().filter(|l| l.contains("mb_classic_sum{")).count(),
            3
        );
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("mb_classic_count{"))
                .count(),
            3
        );

        // Within one step the cumulative counts never decrease, and +Inf is the
        // largest.
        let step_one: Vec<f64> = bucket_lines
            .iter()
            .take(8)
            .map(|l| {
                let payload = l.split('\t').nth(2).unwrap_or_default();
                let bits = payload
                    .strip_prefix("f 0x")
                    .and_then(|h| u64::from_str_radix(h, 16).ok())
                    .expect("bucket payload is hex float bits");
                f64::from_bits(bits)
            })
            .collect();
        assert_eq!(step_one.len(), 8);
        for pair in step_one.windows(2) {
            assert!(
                pair[1] >= pair[0],
                "classic histogram buckets must be cumulative: {step_one:?}"
            );
        }
    }

    #[test]
    fn native_histograms_carry_the_declared_schema_and_bucket_count() {
        let w = workload(5, clean());
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(5)
            .expect("generates");
        let text = String::from_utf8(bytes).expect("utf8");
        let native: Vec<&str> = text
            .lines()
            .filter(|l| l.contains("\th schema="))
            .collect();
        assert_eq!(native.len(), 5);
        assert_eq!(report.native_histogram_samples, 5);
        for line in &native {
            assert!(line.contains("h schema=2 "), "wrong schema: {line}");
            let deltas = line
                .split("deltas=")
                .nth(1)
                .expect("deltas present")
                .split(',')
                .count();
            assert_eq!(deltas, 4, "four positive buckets were declared: {line}");
        }
    }

    #[test]
    fn a_missing_sample_omits_the_whole_instance_and_the_accounting_closes() {
        let w = workload(
            21,
            AnomalyRates {
                missing_sample_one_in: 5,
                stale_marker_one_in: 0,
                counter_reset_one_in: 0,
                out_of_order_one_in: 0,
            },
        );
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(20)
            .expect("generates");
        assert_eq!(report.nominal_samples, 400);
        assert_eq!(report.emitted_samples, 328);
        assert_eq!(report.omitted_missing_samples, 72);
        assert_eq!(
            report.emitted_samples + report.omitted_missing_samples,
            report.nominal_samples,
            "generated must equal emitted plus explicitly reported omissions"
        );
        assert_eq!(
            bytes.iter().filter(|b| **b == b'\n').count() as u64,
            report.emitted_samples
        );
        // A dropped classic-histogram scrape omits all ten of its series, so the
        // omission count is not a multiple of one.
        assert_ne!(report.omitted_missing_samples % 10, 0);
    }

    #[test]
    fn staleness_markers_replace_gauge_values_only() {
        let w = workload(
            33,
            AnomalyRates {
                missing_sample_one_in: 0,
                stale_marker_one_in: 4,
                counter_reset_one_in: 0,
                out_of_order_one_in: 0,
            },
        );
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(20)
            .expect("generates");
        let text = String::from_utf8(bytes).expect("utf8");
        assert_eq!(report.nominal_samples, 400);
        assert_eq!(report.stale_markers, 30);
        assert_eq!(report.float_samples, 350);
        assert_eq!(report.native_histogram_samples, 20);
        assert_eq!(
            report.float_samples + report.stale_markers + report.native_histogram_samples,
            report.emitted_samples
        );
        for line in text.lines().filter(|l| l.ends_with("stale")) {
            assert!(
                line.contains("\tmb_gauge{"),
                "only gauges carry staleness markers: {line}"
            );
        }
    }

    #[test]
    fn a_shifted_sample_lands_at_or_before_a_timestamp_already_in_the_stream() {
        let w = workload(
            44,
            AnomalyRates {
                missing_sample_one_in: 0,
                stale_marker_one_in: 0,
                counter_reset_one_in: 0,
                out_of_order_one_in: 4,
            },
        );
        let (bytes, report) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(20)
            .expect("generates");
        let text = String::from_utf8(bytes).expect("utf8");
        assert_eq!(report.out_of_order_samples, 66);

        // Per series, count the samples whose timestamp is older than one
        // already seen on that series: that is what "out of order in the
        // stream" means to an ingester.
        let mut newest: BTreeMap<String, i64> = BTreeMap::new();
        let mut regressions = 0u64;
        for line in text.lines() {
            let mut parts = line.split('\t');
            let ts: i64 = parts
                .next()
                .and_then(|t| t.parse().ok())
                .expect("line starts with a timestamp");
            let series = parts.next().unwrap_or_default().to_string();
            match newest.get(&series) {
                Some(previous) if ts <= *previous => regressions += 1,
                _ => {
                    newest.insert(series, ts);
                }
            }
        }
        // 62 of the 66 shifted samples land at or before a timestamp already
        // written for their series. The other 4 follow two consecutive shifted
        // steps on the same series, where a two-interval shift still lands
        // ahead of everything written so far, so they are shifted but not
        // out-of-order to an ingester. Both figures are pinned: a generator
        // that stopped shifting moves the first, and one that shifted by the
        // wrong amount moves the second.
        assert_eq!(regressions, 62);
        assert!(
            regressions < report.out_of_order_samples,
            "a shift can only ever be at most as visible as the shifts applied"
        );
        // Steps 0 and 1 are never shifted, so the first two scrapes are clean.
        let interval_ms = 15_000i64;
        for line in text.lines() {
            let ts: i64 = line
                .split('\t')
                .next()
                .and_then(|t| t.parse().ok())
                .expect("timestamp");
            assert!(ts >= 0, "no sample is stamped before the base timestamp");
            assert_eq!(ts % interval_ms, 0, "timestamps stay on the scrape grid");
        }
    }

    #[test]
    fn churn_creates_a_new_cohort_at_each_hour_boundary() {
        let w = workload(77, clean());
        // 240 steps at 15 s is one hour exactly, so step 240 is the first step
        // of epoch 1: 241 steps span two epochs.
        let mut gen_one_hour = Generator::new(&w, "churn", 0).expect("generator builds");
        let (_, one_hour) = gen_one_hour.generate_bytes(240).expect("generates");
        assert_eq!(one_hour.total_series_created, 20, "one epoch, no churn yet");

        let mut gen_two_epochs = Generator::new(&w, "churn", 0).expect("generator builds");
        let (bytes, two_epochs) = gen_two_epochs.generate_bytes(241).expect("generates");
        // 25%/h of each family's instances: gauge 6 -> 1, counter 3 -> 0,
        // classic 1 -> 0, native 1 -> 0. Only the gauge family is large enough
        // for a whole instance to churn at this scale, and one gauge instance is
        // one series.
        assert_eq!(two_epochs.total_series_created, 21);
        assert_eq!(
            two_epochs.active_series, 20,
            "churn replaces series, it does not change how many are alive"
        );
        assert_eq!(two_epochs.nominal_samples, 241 * 20);
        assert_eq!(two_epochs.emitted_samples, 241 * 20);

        let text = String::from_utf8(bytes).expect("utf8");
        let series: std::collections::BTreeSet<&str> = text
            .lines()
            .filter_map(|l| l.split('\t').nth(1))
            .collect();
        assert_eq!(
            series.len(),
            21,
            "the churned-in cohort must be a new series identity, not a renamed one"
        );
    }

    #[test]
    fn an_unknown_profile_is_a_typed_error_naming_what_is_available() {
        let w = workload(1, clean());
        let err = Generator::new(&w, "nope", 0).expect_err("an unknown profile must fail");
        match &err {
            GeneratorError::UnknownProfile { name, available } => {
                assert_eq!(name, "nope");
                assert_eq!(
                    available,
                    &vec![
                        "cardinality".to_string(),
                        "history".to_string(),
                        "churn".to_string(),
                        "ci".to_string()
                    ]
                );
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    #[test]
    fn the_report_carries_the_profiles_publishability() {
        let w = workload(1, clean());
        let (_, ci) = Generator::new(&w, "ci", 0)
            .expect("generator builds")
            .generate_bytes(2)
            .expect("generates");
        assert!(!ci.publishable);
        assert_eq!(ci.non_comparable_reason.as_deref(), Some("too small to measure"));

        let (_, cardinality) = Generator::new(&w, "cardinality", 0)
            .expect("generator builds")
            .generate_bytes(2)
            .expect("generates");
        assert!(cardinality.publishable);
        assert_eq!(cardinality.non_comparable_reason, None);
    }

    #[test]
    fn float_payloads_are_written_as_bit_patterns() {
        // A value whose decimal formatting would lose the distinction, encoded
        // through the shipping encoder rather than a stand-in.
        let sample = GeneratedSample {
            ts_ms: 5,
            metric: "mb_gauge".to_string(),
            labels: vec![("job".to_string(), "api".to_string())],
            value: SampleValue::Float(-0.0),
        };
        let mut out = Vec::new();
        sample.encode(&mut out);
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            "5\tmb_gauge{job=\"api\"}\tf 0x8000000000000000\n"
        );

        let positive = GeneratedSample {
            value: SampleValue::Float(0.0),
            ..sample
        };
        let mut out = Vec::new();
        positive.encode(&mut out);
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            "5\tmb_gauge{job=\"api\"}\tf 0x0000000000000000\n"
        );
    }

    #[test]
    fn mixing_is_a_pure_function_of_its_coordinates() {
        assert_eq!(mix(1, &[2, 3]), mix(1, &[2, 3]));
        assert_ne!(mix(1, &[2, 3]), mix(1, &[3, 2]));
        assert_ne!(mix(1, &[2, 3]), mix(2, &[2, 3]));
        // The scaled value stays inside its declared range and is exactly
        // representable, so a float value reproduces bit for bit.
        for i in 0..64u64 {
            let v = unit_scaled(mix(9, &[i]), 100, 10);
            assert!((0.0..100.0).contains(&v), "{v} out of range");
            assert_eq!(v, f64::from_bits(v.to_bits()));
        }
    }
}
