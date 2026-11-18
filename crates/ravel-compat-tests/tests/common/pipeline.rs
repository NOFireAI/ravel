//! The init, activate, migrate, sweep and certify steps of the retained-data
//! contract (ADR-4233), parameterised over the store each step runs against so
//! a test can put a `FaultStore` or a `TapStore` under one step at a time. The
//! timeline is `e2e.rs`'s: one RSEG 7 metrics object in a sealed hour,
//! activation of the drill version N+1 at `sealed_now_ns()`, and the time gate
//! from the ADR's terms.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prost::Message;
use ravel_catalog::compat::{COMPAT_VIEW_HORIZON_NS, CompatClock, CompatKeys, CompatView};
use ravel_catalog::{
    AbsentPolicy, Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, provisioning_key, validate_or_adopt,
};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    ActivateRequest, CertifyReport, CertifyTimings, CompactorConfig, FamilyMigrateReport,
    FenceCursor, FixedClock, InitConfig, InitOutcome, MaintainCompatPolicy, MigrateBudget,
    NoLeases, SupersededSweepOutcome, WritableVersions, certify, init_compat, migrate_family,
    sweep_superseded,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1 as sysproto;
use ravel_segment::{
    IngestBounds, SegmentIdentity, SegmentVersion, SegmentWriter, SeriesInputV3, SeriesValues,
    VERSION_DRILL,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

use super::DecodedSeries;

pub const SECOND_NS: i64 = 1_000_000_000;
pub const HOUR_NS: i64 = 3_600 * SECOND_NS;
pub const T_REQ_NS: i64 = 60 * SECOND_NS;
/// The hour the seed objects land in; sealed under the migrate clock.
pub const HOUR: u32 = 495_000;
pub const METRIC: &str = "compat_gauge";

#[derive(Debug)]
pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn new(ns: i64) -> Self {
        ManualClock(AtomicI64::new(ns))
    }
    pub fn set(&self, ns: i64) {
        self.0.store(ns, Ordering::SeqCst);
    }
}

impl CompatClock for ManualClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub fn compat_keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[[5; 32]]).expect("one key")
}

pub fn tenant() -> TenantId {
    TenantId::new("acme")
}

pub fn hash() -> TenantHash {
    tenant().hash()
}

pub fn drill() -> u32 {
    u32::from(VERSION_DRILL)
}

pub fn v7() -> u32 {
    u32::from(ravel_segment::VERSION_V7)
}

/// The first instant of [`HOUR`].
pub fn hour_start_ns() -> i64 {
    i64::from(HOUR) * HOUR_NS
}

/// A now past the seal margin for [`HOUR`]; the activation instant.
pub fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * HOUR_NS + 3 * HOUR_NS
}

pub fn series_id() -> SeriesId {
    SeriesId::compute(&tenant(), METRIC, &labels()).expect("series id")
}

fn labels() -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    }])
    .expect("labels")
}

/// One ingest flush: a single-sample RSEG object and its commit record.
#[derive(Debug, Clone)]
pub struct Flush {
    pub shard: u32,
    pub writer: u128,
    pub seq: u64,
    pub version: SegmentVersion,
    /// The sample's timestamp, also the flush's ingest bounds and record
    /// creation time; its hour is the bucket the flush lands in.
    pub ts_ns: i64,
    pub value: f64,
}

impl Flush {
    /// A v7 flush into [`HOUR`] on `shard`.
    pub fn v7(shard: u32, writer: u128, seq: u64, value: f64) -> Self {
        Flush {
            shard,
            writer,
            seq,
            version: SegmentVersion::V7,
            ts_ns: hour_start_ns() + i64::try_from(seq).expect("seq") * SECOND_NS,
            value,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Flushed {
    pub data_key: String,
    pub record_key: String,
    pub data: Vec<u8>,
    pub record: Vec<u8>,
    pub sample_count: u64,
    pub series_count: u64,
}

/// Encode `flush` as an ingest flush would, without writing anything.
pub fn encode_flush(flush: &Flush) -> Flushed {
    let writer_id = Uuid::from_u128(flush.writer);
    let written = SegmentWriter::write_histograms_with_exemplars_at(
        flush.version,
        vec![SeriesInputV3 {
            series_id: series_id(),
            labels: labels(),
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: flush.ts_ns,
                value: flush.value,
            }]),
        }],
        SegmentIdentity {
            tenant_hash: hash().0,
            shard: flush.shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: flush.seq,
        },
        IngestBounds {
            min_ingest_ts_ns: flush.ts_ns,
            max_ingest_ts_ns: flush.ts_ns,
        },
        Vec::new(),
    )
    .expect("write RSEG");
    let data_key = keys::data_key(
        &hash(),
        Signal::Metrics,
        flush.shard,
        writer_id,
        1,
        flush.seq,
        &written.summary.blake3,
    )
    .expect("data key");
    let commit = record::build(NewCommitRecord {
        tenant_hash: hash(),
        signal: Signal::Metrics,
        shard: flush.shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: flush.seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: flush.ts_ns,
        max_ingest_ts_ns: flush.ts_ns,
        segment_format_version: u32::from(flush.version.number()),
        created_unix_ns: flush.ts_ns,
        ingest_hour_bucket: u32::try_from(flush.ts_ns.div_euclid(HOUR_NS)).expect("hour"),
    })
    .expect("build commit record");
    Flushed {
        data_key,
        record_key: keys::commit_key_for_record(&commit).expect("commit key"),
        data: written.bytes.to_vec(),
        record: record::encode(&commit).to_vec(),
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
    }
}

/// Write `flush`'s data object, then its commit record.
pub async fn write_flush(store: &dyn ObjectStoreBackend, flush: &Flush) -> Flushed {
    let flushed = encode_flush(flush);
    store
        .put(
            &flushed.data_key,
            flushed.data.clone().into(),
            PutOptions::default(),
        )
        .await
        .expect("put data object");
    store
        .put(
            &flushed.record_key,
            flushed.record.clone().into(),
            PutOptions::default(),
        )
        .await
        .expect("put commit record");
    flushed
}

pub async fn provision(store: &dyn ObjectStoreBackend, shard_count: u32) {
    validate_or_adopt(
        store,
        &hash(),
        Signal::Metrics,
        shard_count,
        hour_start_ns(),
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
}

pub async fn init(store: &dyn ObjectStoreBackend, clock: &ManualClock) {
    let init = init_compat(
        store,
        Some(&compat_keys()),
        clock,
        &InitConfig {
            baseline_release_major: 0,
            baseline_release_minor: 34,
            max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        },
    )
    .await
    .expect("init");
    assert_eq!(init.outcome, InitOutcome::Created);
}

pub fn writable() -> WritableVersions {
    WritableVersions::current().with_family(ravel_catalog::compat::FAMILY_RSEG, vec![v7(), drill()])
}

pub fn activate_request(resume_from: Option<FenceCursor>) -> ActivateRequest {
    ActivateRequest {
        family: ravel_catalog::compat::FAMILY_RSEG.to_string(),
        version: drill(),
        activated_by: "compat test".to_string(),
        writer_roles_can_read_compat: true,
        alert_rule_tenants: Vec::new(),
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        resume_from,
    }
}

/// A compactor configuration whose compat view reads `view_store`.
pub async fn compactor_config(
    view_store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<ManualClock>,
) -> CompactorConfig {
    let view = CompatView::new(view_store, compat_keys(), clock.clone());
    view.refresh().await.expect("refresh the view");
    CompactorConfig {
        compat_policy: Some(MaintainCompatPolicy::new(view, clock)),
        ..CompactorConfig::default()
    }
}

pub async fn migrate(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    shard_count: u32,
) -> ravel_maintain::Result<FamilyMigrateReport> {
    migrate_family(
        store,
        &FixedClock::new(sealed_now_ns()),
        config,
        hash(),
        Signal::Metrics,
        ravel_catalog::compat::FAMILY_RSEG,
        drill(),
        shard_count,
        MigrateBudget::unlimited(),
        "compat test migrate",
    )
    .await
}

pub async fn sweep(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    now_ns: i64,
    shard: u32,
) -> ravel_maintain::Result<SupersededSweepOutcome> {
    sweep_superseded(
        store,
        &FixedClock::new(now_ns),
        config,
        &NoLeases,
        &hash(),
        Signal::Metrics,
        shard,
    )
    .await
}

/// The two sweep instants after which a superseded input is gone: the first
/// pass stamps the pinned-window marker, the second deletes.
pub fn sweep_times(config: &CompactorConfig) -> (i64, i64) {
    let first = (not_before_ns() + 1).max(sealed_now_ns() + config.protection_horizon_ns + 1);
    let window = config.max_query_duration_ns
        + config.head_cache_ttl_ns
        + 4 * config.clock_skew_allowance_ns;
    (first, first + window)
}

pub fn timings() -> CertifyTimings {
    CertifyTimings {
        t_req_ns: T_REQ_NS,
        max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    }
}

/// Certify's time gate for a bucket fenced at activation (`sealed_now_ns()`),
/// from the ADR's terms (decision 7).
pub fn not_before_ns() -> i64 {
    let activation = sealed_now_ns();
    let p_ns = DEFAULT_MAX_FLUSH_LIFETIME_NS;
    let activation_term = activation + COMPAT_VIEW_HORIZON_NS + p_ns + T_REQ_NS;
    let g_pre = 3 * HOUR_NS + 2 * DEFAULT_MAX_FLUSH_LIFETIME_NS + p_ns + T_REQ_NS;
    activation_term.max(activation + g_pre) + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
}

pub async fn certify_at(
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
    budget: Option<usize>,
) -> ravel_maintain::Result<CertifyReport> {
    certify(
        store,
        &compat_keys(),
        now_ns,
        ravel_catalog::compat::FAMILY_RSEG,
        drill(),
        timings(),
        budget,
    )
    .await
}

pub async fn provisioning(store: &dyn ObjectStoreBackend) -> sysproto::ProvisioningRecord {
    sysproto::ProvisioningRecord::decode(
        store
            .get(&provisioning_key(&hash(), Signal::Metrics), GetRange::Full)
            .await
            .expect("get provisioning")
            .data
            .as_ref(),
    )
    .expect("decode provisioning")
}

/// The data objects a query over [`HOUR`] resolves, each with its decoded
/// series, in key order.
pub async fn query_hour(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    now_ns: i64,
) -> Vec<(String, Vec<DecodedSeries>)> {
    query_hours(store, shard_count, HOUR, 1, now_ns).await
}

/// The data objects a query over `hours` hours from `first_hour` resolves,
/// each with its decoded series, in key order.
pub async fn query_hours(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    first_hour: u32,
    hours: u32,
    now_ns: i64,
) -> Vec<(String, Vec<DecodedSeries>)> {
    let catalog = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(
            &hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: i64::from(first_hour) * HOUR_NS,
                end_ns: i64::from(first_hour + hours) * HOUR_NS,
            },
            &[],
            now_ns,
        )
        .await
        .expect("resolve the query snapshot");
    let mut out = Vec::new();
    for segment in snapshot.segments {
        let bytes = store
            .get(&segment.data_object_key, GetRange::Full)
            .await
            .expect("get a resolved object")
            .data;
        out.push((
            segment.data_object_key,
            super::decode_rseg(&bytes).expect("decode a resolved object"),
        ));
    }
    out.sort();
    out
}
