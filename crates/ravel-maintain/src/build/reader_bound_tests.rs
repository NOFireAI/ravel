//! The reader-bound part split (issue #4645), driven through the whole
//! `compact_bucket` pipeline over a `MemoryStore` with small reader limits
//! substituted for the defaults the bound is derived from. Every part is then
//! decoded under those same limits, so a part the split let through that its
//! reader refuses fails here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::CompactionRecord;
use ravel_segment::{
    IngestBounds, PROVENANCE_LIVE_BYTES_PER_SAMPLE, ReaderLimits, SegmentIdentity, SegmentWriter,
    SeriesEntryV4, SeriesInputV3, SeriesValues, V5_SPARSE_THRESHOLD, VERSION_V7, decode_catalog_v5,
    decode_run_pages_soa, max_provenance_samples, max_run_total, open_from_full, plan_ranges_v4,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

use super::TEST_PART_READER_LIMITS;
use crate::bucket::Bucket;
use crate::compact::{CompactionOutcome, compact_bucket};
use crate::error::MaintainError;
use crate::{CompactorConfig, FixedClock};

const TENANT: &str = "acme";
const SHARD: u32 = 7;
const HOUR: u32 = 495_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const EPOCH: u64 = 10;
/// Samples in each series that reaches L1 from one input run, and so carries
/// no provenance column.
const PLAIN_SAMPLES: usize = 5;

/// Reader limits whose provenance bound is exactly `bound`: the section cap
/// sits one byte short of admitting `bound + 1` flagged samples.
fn limits_for_provenance_bound(bound: u64) -> ReaderLimits {
    ReaderLimits {
        max_section_uncompressed_bytes: (bound + 1) * PROVENANCE_LIVE_BYTES_PER_SAMPLE - 1,
        ..ReaderLimits::default()
    }
}

/// Builds parts on this thread under `limits` instead of the defaults.
/// `#[tokio::test]` runs the whole pipeline on the test's own thread.
struct LimitsOverride;

impl LimitsOverride {
    fn set(limits: ReaderLimits) -> Self {
        TEST_PART_READER_LIMITS.with(|cell| cell.set(Some(limits)));
        Self
    }
}

impl Drop for LimitsOverride {
    fn drop(&mut self) {
        TEST_PART_READER_LIMITS.with(|cell| cell.set(None));
    }
}

fn tenant_hash() -> ravel_types::TenantHash {
    TenantId::new(TENANT).hash()
}

fn bucket() -> Bucket {
    bucket_at(HOUR)
}

fn bucket_at(hour: u32) -> Bucket {
    Bucket::new(tenant_hash(), Signal::Metrics, SHARD, hour)
}

fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
}

fn labels(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// `count` metric names in ascending series-id order, the order parts are
/// cut in, so a test can place each series' role by its position.
fn metrics_in_id_order(count: usize) -> Vec<(SeriesId, String)> {
    let tenant = TenantId::new(TENANT);
    let mut out: Vec<(SeriesId, String)> = (0..count)
        .map(|i| {
            let metric = format!("m{i:05}");
            let id = SeriesId::compute(&tenant, &metric, &labels(&metric)).expect("series id");
            (id, metric)
        })
        .collect();
    out.sort_by_key(|(id, _)| id.0);
    out
}

/// One output series' input: `flagged == 0` is a plain series written by one
/// input (alternating between the two by position), otherwise `flagged`
/// samples split across two inputs, so the L1 run merges them and carries one
/// provenance entry per sample.
struct Plan {
    id: SeriesId,
    metric: String,
    flagged: usize,
}

fn plans(flagged_by_position: &[usize]) -> Vec<Plan> {
    metrics_in_id_order(flagged_by_position.len())
        .into_iter()
        .zip(flagged_by_position)
        .map(|((id, metric), &flagged)| Plan {
            id,
            metric,
            flagged,
        })
        .collect()
}

/// One sample as `(series id, ts, value bits)`.
type Row = ([u8; 16], i64, u64);

fn sample(hour: u32, position: usize, k: usize) -> Sample {
    Sample {
        ts_ns: i64::from(hour) * NS_PER_HOUR + k as i64,
        value: (position * 100_000 + k) as f64,
    }
}

/// The two L0 inputs' series and every sample the compaction must carry, as
/// `(series id, ts, value bits)` sorted.
fn inputs(plans: &[Plan]) -> ([Vec<SeriesInputV3>; 2], Vec<Row>) {
    inputs_at(HOUR, plans)
}

/// [`inputs`] for the bucket at `hour`.
fn inputs_at(hour: u32, plans: &[Plan]) -> ([Vec<SeriesInputV3>; 2], Vec<Row>) {
    let mut by_input: [Vec<SeriesInputV3>; 2] = [Vec::new(), Vec::new()];
    let mut rows = Vec::new();
    for (position, plan) in plans.iter().enumerate() {
        let samples: Vec<Sample> = if plan.flagged == 0 {
            (0..PLAIN_SAMPLES)
                .map(|k| sample(hour, position, k))
                .collect()
        } else {
            (0..plan.flagged)
                .map(|k| sample(hour, position, k))
                .collect()
        };
        rows.extend(
            samples
                .iter()
                .map(|s| (plan.id.0, s.ts_ns, s.value.to_bits())),
        );
        let halves: [Vec<Sample>; 2] = if plan.flagged == 0 && position % 2 == 0 {
            [samples, Vec::new()]
        } else if plan.flagged == 0 {
            [Vec::new(), samples]
        } else {
            let (even, odd) = samples
                .iter()
                .enumerate()
                .partition::<Vec<_>, _>(|(k, _)| k % 2 == 0);
            [
                even.into_iter().map(|(_, s)| *s).collect(),
                odd.into_iter().map(|(_, s)| *s).collect(),
            ]
        };
        for (input, half) in by_input.iter_mut().zip(halves) {
            if !half.is_empty() {
                input.push(SeriesInputV3 {
                    series_id: plan.id,
                    labels: labels(&plan.metric),
                    values: SeriesValues::Scalar(half),
                });
            }
        }
    }
    rows.sort_unstable();
    (by_input, rows)
}

/// Seeds one L0 input (data object and commit record); returns its data key.
async fn seed(store: &dyn ObjectStoreBackend, seq: u64, series: Vec<SeriesInputV3>) -> String {
    seed_at(store, HOUR, seq, series).await
}

/// [`seed`] into the bucket at `hour`.
async fn seed_at(
    store: &dyn ObjectStoreBackend,
    hour: u32,
    seq: u64,
    series: Vec<SeriesInputV3>,
) -> String {
    let th = tenant_hash();
    let writer_id = Uuid::from_u128(u128::from(seq));
    let created = i64::from(hour) * NS_PER_HOUR + (seq as i64) * 1_000_000;
    let identity = SegmentIdentity {
        tenant_hash: th.0,
        shard: SHARD,
        writer_id: writer_id.to_string(),
        writer_epoch: EPOCH,
        writer_seq: seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
    };
    let written =
        SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, Vec::new())
            .expect("write L0");
    let content_hash = written.summary.blake3;
    let data_key = keys::data_key(
        &th,
        Signal::Metrics,
        SHARD,
        writer_id,
        EPOCH,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put data object");
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Metrics,
        shard: SHARD,
        writer_id,
        writer_epoch: EPOCH,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: created,
        ingest_hour_bucket: hour,
    })
    .expect("build commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put commit record");
    data_key
}

async fn seed_both(store: &dyn ObjectStoreBackend, plans: &[Plan]) -> Vec<Row> {
    let ([first, second], rows) = inputs(plans);
    seed(store, 1, first).await;
    seed(store, 2, second).await;
    rows
}

async fn compact(store: &MemoryStore) -> Result<CompactionOutcome, MaintainError> {
    let clock = FixedClock::new(sealed_now_ns());
    compact_bucket(store, &clock, &CompactorConfig::default(), &bucket()).await
}

/// Every L1 part's bytes, in record (ascending series-range) order.
async fn read_parts(store: &dyn ObjectStoreBackend) -> Vec<Bytes> {
    use prost::Message;
    let listing = crate::read::list_bucket(store, &bucket())
        .await
        .expect("list");
    assert_eq!(
        listing.compaction_record_keys.len(),
        1,
        "expected exactly one compaction record"
    );
    let got = store
        .get(&listing.compaction_record_keys[0], GetRange::Full)
        .await
        .expect("get record");
    let record = CompactionRecord::decode(got.data.as_ref()).expect("decode record");
    let mut parts = Vec::new();
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
        parts.push(
            store
                .get(&key, GetRange::Full)
                .await
                .expect("get part")
                .data,
        );
    }
    parts
}

/// One decoded part: its catalog under `limits`, as its readers see it.
struct DecodedPart {
    series: Vec<SeriesEntryV4>,
    flagged: u64,
}

fn decode_part(bytes: &[u8], limits: ReaderLimits) -> DecodedPart {
    let loc = open_from_full(bytes, limits).expect("part opens under the bound's limits");
    let series = decode_catalog_v5(&loc.footer, bytes, limits)
        .expect("part decodes under the bound's limits");
    let flagged = series
        .iter()
        .flat_map(|s| s.per_sample_provenance.iter().flatten())
        .map(|column| column.len() as u64)
        .sum();
    DecodedPart { series, flagged }
}

/// Every sample of `part` as `(series id, ts, value bits)`.
fn part_rows(bytes: &[u8], part: &DecodedPart, limits: ReaderLimits) -> Vec<Row> {
    let page = |(offset, len): (u64, u64)| &bytes[offset as usize..(offset + len) as usize];
    let footer = &open_from_full(bytes, limits).expect("open part").footer;
    let refs: Vec<&SeriesEntryV4> = part.series.iter().collect();
    let planned = plan_ranges_v4(footer, &refs).expect("plan ranges");
    let mut planned = planned.iter();
    let mut rows = Vec::new();
    let mut scratch = Vec::new();
    for series in &part.series {
        for run in &series.runs {
            let range = planned.next().expect("one planned range per run");
            let (mut ts, mut values) = (Vec::new(), Vec::new());
            decode_run_pages_soa(
                &series.entry.series_id,
                run,
                page(range.ts_range),
                page(range.val_range),
                limits,
                &mut scratch,
                &mut ts,
                &mut values,
            )
            .expect("decode run pages");
            rows.extend(
                ts.into_iter()
                    .zip(values)
                    .map(|(t, v)| (series.entry.series_id.0, t, v.to_bits())),
            );
        }
    }
    rows
}

/// Flagged samples per series in id order, with plain series between merged
/// ones, totalling three times the bound. Each part closes exactly at the
/// bound because the next merged series would take it one or more over: a
/// check made after adding a series puts that series in the earlier part,
/// and a check that also counted the plain series' samples would close each
/// part one series sooner.
#[tokio::test]
async fn parts_split_before_the_series_that_would_pass_the_provenance_bound() {
    const BOUND: u64 = 1_000;
    let limits = limits_for_provenance_bound(BOUND);
    assert_eq!(max_provenance_samples(limits), BOUND);
    assert!(max_run_total(limits) > 9, "only the provenance bound binds");
    let _limits = LimitsOverride::set(limits);

    let store = MemoryStore::new();
    let plans = plans(&[400, 0, 600, 300, 0, 700, 2, 0, 998]);
    let rows = seed_both(&store, &plans).await;

    let outcome = compact(&store).await.expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    let parts = read_parts(&store).await;
    let decoded: Vec<DecodedPart> = parts.iter().map(|p| decode_part(p, limits)).collect();
    assert_eq!(
        decoded.iter().map(|p| p.flagged).collect::<Vec<_>>(),
        vec![BOUND, BOUND, BOUND],
        "three parts, each at exactly the bound"
    );
    assert_eq!(
        decoded.iter().map(|p| p.series.len()).collect::<Vec<_>>(),
        vec![3, 3, 3]
    );
    for part in &decoded {
        assert!((part.series.len() as u64) < V5_SPARSE_THRESHOLD);
    }

    let mut got: Vec<_> = parts
        .iter()
        .zip(&decoded)
        .flat_map(|(bytes, part)| part_rows(bytes, part, limits))
        .collect();
    got.sort_unstable();
    assert_eq!(got, rows, "every input sample reaches exactly one part");
}

/// A part large enough for the chunked catalog closes at the bound too, and
/// its sparse decode accepts it under the same limits. The section cap is
/// also what bounds the run total, so it is sized to admit the 4099-series
/// first part.
#[tokio::test]
async fn a_chunked_catalog_part_at_the_bound_decodes_on_the_sparse_path() {
    const BOUND: u64 = 16_000;
    let limits = limits_for_provenance_bound(BOUND);
    assert_eq!(max_provenance_samples(limits), BOUND);
    assert!(
        max_run_total(limits) >= 4_103,
        "only the provenance bound binds"
    );
    let _limits = LimitsOverride::set(limits);

    let plain = V5_SPARSE_THRESHOLD as usize + 1;
    let mut flagged = vec![8_000];
    flagged.extend(std::iter::repeat_n(0, plain));
    flagged.extend([8_000, 1_000, 0, 0, 0]);
    let store = MemoryStore::new();
    let plans = plans(&flagged);
    let rows = seed_both(&store, &plans).await;

    compact(&store).await.expect("compact");
    let parts = read_parts(&store).await;
    let decoded: Vec<DecodedPart> = parts.iter().map(|p| decode_part(p, limits)).collect();
    assert_eq!(
        decoded.iter().map(|p| p.flagged).collect::<Vec<_>>(),
        vec![BOUND, 1_000]
    );
    assert_eq!(
        decoded.iter().map(|p| p.series.len()).collect::<Vec<_>>(),
        vec![plain + 2, 4]
    );
    assert!(
        (decoded[0].series.len() as u64) >= V5_SPARSE_THRESHOLD,
        "sparse path"
    );

    let mut got: Vec<_> = parts
        .iter()
        .zip(&decoded)
        .flat_map(|(bytes, part)| part_rows(bytes, part, limits))
        .collect();
    got.sort_unstable();
    assert_eq!(got, rows);
}

/// With no provenance at all, the run total alone closes parts.
#[tokio::test]
async fn parts_split_before_the_series_that_would_pass_the_run_bound() {
    let limits = ReaderLimits {
        max_section_uncompressed_bytes: 3 * 240 + 239,
        ..ReaderLimits::default()
    };
    assert_eq!(max_run_total(limits), 3);
    let _limits = LimitsOverride::set(limits);

    let store = MemoryStore::new();
    let plans = plans(&[0; 7]);
    let rows = seed_both(&store, &plans).await;

    compact(&store).await.expect("compact");
    let parts = read_parts(&store).await;
    let decoded: Vec<DecodedPart> = parts.iter().map(|p| decode_part(p, limits)).collect();
    assert_eq!(
        decoded.iter().map(|p| p.series.len()).collect::<Vec<_>>(),
        vec![3, 3, 1]
    );
    let mut got: Vec<_> = parts
        .iter()
        .zip(&decoded)
        .flat_map(|(bytes, part)| part_rows(bytes, part, limits))
        .collect();
    got.sort_unstable();
    assert_eq!(got, rows);
}

/// One series over the bound on its own cannot be split: the bucket is refused
/// with the typed error, writes nothing, and its L0 inputs stay listed and
/// readable.
#[tokio::test]
async fn a_single_series_over_the_bound_refuses_the_bucket_and_writes_nothing() {
    const BOUND: u64 = 1_000;
    let limits = limits_for_provenance_bound(BOUND);
    let _limits = LimitsOverride::set(limits);

    let store = MemoryStore::new();
    let plans = plans(&[0, 1_001, 0]);
    let ([first, second], _) = inputs(&plans);
    let data_keys = vec![seed(&store, 1, first).await, seed(&store, 2, second).await];
    let before: Vec<String> = list_all(&store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect();

    let err = compact(&store).await.expect_err("over-bound series");
    match err {
        MaintainError::SeriesOverPartReaderBound {
            series_id,
            quantity,
            count,
            bound,
        } => {
            assert_eq!(series_id, plans[1].id.to_hex());
            assert_eq!(quantity, "flagged provenance samples");
            assert_eq!((count, bound), (1_001, BOUND));
        }
        other => panic!("expected SeriesOverPartReaderBound, got {other:?}"),
    }

    let after: Vec<String> = list_all(&store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert_eq!(after, before, "no part, record or marker was written");
    let listing = crate::read::list_bucket(&store, &bucket())
        .await
        .expect("list bucket");
    assert!(listing.compaction_record_keys.is_empty());
    assert_eq!(listing.commit_keys.len(), 2, "both L0 inputs stay live");
    for key in &data_keys {
        let got = store.get(key, GetRange::Full).await.expect("L0 input");
        open_from_full(&got.data, ReaderLimits::default()).expect("L0 input still reads");
    }
}

/// The three sealed hours of the shard-scan tests: the middle one holds one
/// series over the bound.
const HELD_HOURS: [u32; 3] = [HOUR - 2, HOUR - 1, HOUR];
const OVER_BOUND_HOUR: u32 = HOUR - 1;
const SHARD_BOUND: u64 = 1_000;
const OVER_BOUND_COUNT: u64 = 1_001;

/// Seeds two L0 inputs into each of [`HELD_HOURS`]: the first and third
/// hours carry series under [`SHARD_BOUND`], the middle hour one series of
/// [`OVER_BOUND_COUNT`] flagged samples. Returns the over-bound series' hex
/// id and the middle hour's data keys.
async fn seed_three_hours(store: &dyn ObjectStoreBackend) -> (String, Vec<String>) {
    let mut over = None;
    let mut held_data = Vec::new();
    for (i, hour) in HELD_HOURS.into_iter().enumerate() {
        let flagged: &[usize] = if hour == OVER_BOUND_HOUR {
            &[0, OVER_BOUND_COUNT as usize, 0]
        } else {
            &[0, 400, 0]
        };
        let plans = plans(flagged);
        let ([first, second], _) = inputs_at(hour, &plans);
        let seq = 10 * (i as u64 + 1);
        let a = seed_at(store, hour, seq, first).await;
        let b = seed_at(store, hour, seq + 1, second).await;
        if hour == OVER_BOUND_HOUR {
            over = Some(plans[1].id.to_hex());
            held_data = vec![a, b];
        }
    }
    (over.expect("the middle hour is seeded"), held_data)
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> std::collections::BTreeSet<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

/// The compaction record key and every part key of the one compaction
/// record at `hour`.
async fn published_keys(store: &dyn ObjectStoreBackend, hour: u32) -> Vec<String> {
    use prost::Message;
    let listing = crate::read::list_bucket(store, &bucket_at(hour))
        .await
        .expect("list bucket");
    assert_eq!(
        listing.compaction_record_keys.len(),
        1,
        "hour {hour}: one compaction record"
    );
    let record_key = listing.compaction_record_keys[0].clone();
    let got = store
        .get(&record_key, GetRange::Full)
        .await
        .expect("get record");
    let record = CompactionRecord::decode(got.data.as_ref()).expect("decode record");
    assert_eq!(record.inputs.len(), 2, "hour {hour}: both inputs compacted");
    let mut out = vec![record_key];
    for p in &record.parts {
        out.push(keys::reconstruct_l1_part_key(&record, p).expect("part key"));
    }
    out
}

/// The middle hour holds no compaction record, and both its L0 inputs stay
/// listed and readable.
async fn assert_held_hour_untouched(store: &dyn ObjectStoreBackend, data_keys: &[String]) {
    let listing = crate::read::list_bucket(store, &bucket_at(OVER_BOUND_HOUR))
        .await
        .expect("list bucket");
    assert!(listing.compaction_record_keys.is_empty(), "{listing:?}");
    assert_eq!(listing.commit_keys.len(), 2, "both L0 inputs stay live");
    for key in data_keys {
        let got = store.get(key, GetRange::Full).await.expect("L0 input");
        open_from_full(&got.data, ReaderLimits::default()).expect("L0 input still reads");
    }
}

#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    /// Installs a WARN capture on this thread for the guard's lifetime.
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let writer = self.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish();
        // A second, process-wide dispatcher keeps a callsite's interest
        // computed across every live dispatcher rather than cached from the
        // first thread to reach it.
        static KEEP_TWO_DISPATCHERS: std::sync::OnceLock<tracing::Dispatch> =
            std::sync::OnceLock::new();
        KEEP_TWO_DISPATCHERS
            .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        tracing::subscriber::set_default(subscriber)
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log lock")).into_owned()
    }
}

/// The one warning line the held bucket logs, naming its hour, the series,
/// the quantity, the count and the bound.
fn assert_held_warning(log: &str, series_id: &str) {
    let lines: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("one series alone is over the reader bound of one part"))
        .collect();
    assert_eq!(lines.len(), 1, "exactly one held-bucket warning:\n{log}");
    let line = lines[0];
    for field in [
        format!("ingest_hour_bucket={OVER_BOUND_HOUR}"),
        format!("series_id={series_id}"),
        "quantity=\"flagged provenance samples\"".to_string(),
        format!("count={OVER_BOUND_COUNT}"),
        format!("bound={SHARD_BOUND}"),
    ] {
        assert!(line.contains(&field), "{field} missing from: {line}");
    }
}

/// A shard scan holds the hour whose series is over the bound and still
/// compacts the hour after it: the scan returns Ok, the first and third hours
/// are published, the middle hour writes nothing, and the cursor stops below
/// it. A second pass over the same state refuses the middle hour again and
/// writes no object.
///
/// Before the scan's reader-bound arm, the refusal reached the hard-error arm
/// and the scan returned `Err` before the third hour; a scan that held the
/// hour but stopped there would report one compaction, not two.
#[tokio::test]
async fn a_shard_scan_holds_the_over_bound_hour_and_compacts_the_later_one() {
    let _limits = LimitsOverride::set(limits_for_provenance_bound(SHARD_BOUND));
    let store = MemoryStore::new();
    let (series_id, held_data) = seed_three_hours(&store).await;
    let before = all_keys(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let scan = || {
        crate::scan::scan_and_compact(
            &store,
            &clock,
            &config,
            tenant_hash(),
            Signal::Metrics,
            SHARD,
        )
    };

    let log = Captured::default();
    let guard = log.install();
    let report = scan().await.expect("a held bucket does not fail the scan");
    drop(guard);
    assert_eq!(
        report,
        crate::ScanReport {
            compacted: 2,
            already_done: 0,
            not_sealed: 0,
            held_out_of_window: 0,
            held_unknown_section: 0,
            geometry_unsupported: 0,
            held_reader_bound: 1,
            cursor_advanced_to: Some(HELD_HOURS[0]),
        }
    );
    assert_held_warning(&log.text(), &series_id);

    let mut expected = before.clone();
    expected.insert(
        keys::maint_cursor_key(&tenant_hash(), Signal::Metrics, SHARD).expect("cursor key"),
    );
    for hour in [HELD_HOURS[0], HELD_HOURS[2]] {
        expected.extend(published_keys(&store, hour).await);
    }
    let after = all_keys(&store).await;
    assert_eq!(
        after, expected,
        "only the outer hours' records and parts and the cursor were written"
    );
    assert_held_hour_untouched(&store, &held_data).await;

    let report = scan().await.expect("the second pass holds it again");
    assert_eq!(
        report,
        crate::ScanReport {
            compacted: 0,
            already_done: 1,
            not_sealed: 0,
            held_out_of_window: 0,
            held_unknown_section: 0,
            geometry_unsupported: 0,
            held_reader_bound: 1,
            cursor_advanced_to: None,
        }
    );
    assert_eq!(
        all_keys(&store).await,
        after,
        "the second pass wrote nothing"
    );
}

/// The maintenance loop's claimed path holds the same hour: the pass counts
/// it under `held_reader_bound`, compacts both other hours, logs the series,
/// and marks the held hour's claim completed rather than leaving it to age
/// out under its lease. Without the claim-completing arm in
/// `compact_bucket_scoped`, the held hour's claim stays `Running`.
#[tokio::test]
async fn the_maintain_pass_holds_the_over_bound_hour_and_completes_its_claim() {
    use prost::Message;
    use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
    use ravel_proto::sys::v1::{ClaimState, CompactionClaim};

    let _limits = LimitsOverride::set(limits_for_provenance_bound(SHARD_BOUND));
    let store = MemoryStore::new();
    let (series_id, held_data) = seed_three_hours(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig {
        claim_participant: Some(crate::ClaimParticipant::new(
            Uuid::from_u128(77),
            std::sync::Arc::new(clock.clone()) as std::sync::Arc<dyn crate::Clock>,
        )),
        ..CompactorConfig::default()
    };

    let log = Captured::default();
    let guard = log.install();
    let report = crate::scan::scan_and_maintain(
        &store,
        &clock,
        &config,
        &crate::config::RetentionConfig::default(),
        &crate::NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("a held bucket does not fail the pass");
    drop(guard);
    assert_eq!(report.compacted, 2, "{report:?}");
    assert_eq!(report.held_reader_bound, 1, "{report:?}");
    assert_eq!(report.geometry_unsupported, 0, "{report:?}");
    assert_eq!(report.claims_acquired, 2, "{report:?}");
    assert_held_warning(&log.text(), &series_id);
    for hour in [HELD_HOURS[0], HELD_HOURS[2]] {
        published_keys(&store, hour).await;
    }
    assert_held_hour_untouched(&store, &held_data).await;

    let mut states = Vec::new();
    for meta in list_all(&store, COMPACTION_CLAIMS_PREFIX)
        .await
        .expect("list claims")
    {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get claim");
        let claim = CompactionClaim::decode(got.data.as_ref()).expect("claim decodes");
        states.push(ClaimState::try_from(claim.state).expect("known state"));
    }
    assert_eq!(states, vec![ClaimState::Completed; 3]);
}
