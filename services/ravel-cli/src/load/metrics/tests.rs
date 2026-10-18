use super::*;
use crate::load::test_support::*;

/// The metrics row path reads its name and label dictionary columns in
/// place, and reads the same rows a plain copy of the batch reads, a null
/// label key omitting its label.
#[test]
fn the_metrics_row_path_reads_dictionary_columns_in_place() {
    const TEXT: &str = "[metrics]\nname_column = \"metric\"\nvalue_column = \"value\"\n\
                            ts_column = \"ts\"\nts_unit = \"nanos\"\nkind = \"gauge\"\n\n\
                            [[metrics.label]]\nname = \"host\"\ncolumn = \"host\"\n";
    let MappingSection::Metrics(mapping) =
        parse_mapping_document(TEXT, SignalArg::Metrics).expect("valid mapping")
    else {
        panic!("a [metrics] section parses as metrics");
    };
    let names = vec![Some("cpu"), Some("mem"), Some("cpu")];
    let hosts = vec![Some("a"), None, Some("b")];
    let columns = |metric: ArrayRef, host: ArrayRef| {
        batch(vec![
            ("ts", i64_col(vec![NOW_NS; 3])),
            (
                "value",
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            ),
            ("metric", metric),
            ("host", host),
        ])
    };
    let dict = columns(opt_dict_col(names.clone()), opt_dict_col(hosts.clone()));
    let plain = columns(
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(hosts)),
    );

    let cols = MetricsColumnIndex::resolve(&dict, &mapping).expect("columns resolve");
    for name in ["metric", "host"] {
        let i = dict.schema().index_of(name).expect("a mapped column");
        let (read_from, _) = cols.cell(&dict, i, 0).expect("the cell reads");
        assert!(
            Arc::ptr_eq(read_from, dict.column(i).as_any_dictionary().values()),
            "the metrics row path reads {name} out of the batch's own dictionary values"
        );
    }

    let limits = IngestLimits::default();
    let rows = |b: &RecordBatch| -> Vec<(String, Vec<Label>)> {
        let cols = MetricsColumnIndex::resolve(b, &mapping).expect("columns resolve");
        (0..b.num_rows())
            .map(|row| {
                let r = build_metric_row(
                    b,
                    &cols,
                    &mapping,
                    &limits,
                    &TenantRules::none(),
                    NOW_NS,
                    row,
                )
                .expect("the row builds");
                (r.name, r.labels)
            })
            .collect()
    };
    let got = rows(&dict);
    assert_eq!(
        got,
        rows(&plain),
        "the dictionary batch reads the plain rows"
    );
    assert_eq!(
        got.iter().map(|(_, l)| l.len()).collect::<Vec<_>>(),
        vec![1, 0, 1],
        "the null host key omits its label"
    );
}

/// The metrics loader's series identity against `ravel_otlp::normalize`'s,
/// for a gauge, a monotonic counter and a classic histogram.
///
/// This is the property the changelog promises and the reason the loader
/// calls ravel-otlp's own `sanitize_metric_name`, `sanitize_label_name`
/// and `prometheus_family_name` rather than storing what the mapping says:
/// a metric bulk-loaded from Parquet and the same metric admitted over
/// OTLP must be ONE series, not two. Both sides use a dotted metric name,
/// a dotted attribute key and a unit, which is exactly what the loader
/// used to store raw.
///
/// The OTLP side is the real `normalize_metrics` entry point over real
/// OTLP messages, not a hand-built expectation: an expectation written
/// from the loader's own helpers would agree with any bug they share.
mod otlp_series_identity_parity {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric::Data as MetricData,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use ravel_otlp::normalize_metrics;

    use super::*;

    const METRIC: &str = "http.server.duration";
    const ATTR_KEY: &str = "http.method";
    const ATTR_VALUE: &str = "GET";
    const UNIT: &str = "s";
    const EVENT_NS: i64 = NOW_NS;

    fn tenant() -> TenantId {
        TenantId::new("acme")
    }

    fn attributes() -> Vec<KeyValue> {
        vec![KeyValue {
            key: ATTR_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(ATTR_VALUE.to_string())),
            }),
            ..Default::default()
        }]
    }

    /// One `ExportMetricsServiceRequest` carrying `data` under the shared
    /// name and unit, through a resource with no attributes (so no `job`
    /// or `instance` label enters on either side).
    fn request(data: MetricData) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: METRIC.to_string(),
                        unit: UNIT.to_string(),
                        data: Some(data),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Every series id the OTLP path produced, sorted, with the rejections
    /// asserted empty: a normalize that rejected everything would
    /// otherwise "match" a loader that produced nothing.
    fn otlp_series_ids(data: MetricData) -> Vec<SeriesId> {
        let out = normalize_metrics(
            &tenant(),
            request(data),
            &IngestLimits::default(),
            &ravel_ipcrypt::rules::TenantRules::none(),
            NOW_NS,
        );
        assert!(
            out.rejected.is_empty(),
            "the OTLP fixture must be admitted whole, got {:?}",
            out.rejected
        );
        assert!(
            !out.points.is_empty(),
            "the OTLP fixture must produce at least one point"
        );
        let mut ids: Vec<SeriesId> = out.points.iter().map(|p| p.series_id).collect();
        ids.sort_unstable();
        ids
    }

    /// The same figures through the loader: its series ids, sorted, plus
    /// the points themselves for the caller to inspect.
    fn loader_points(mapping: &MetricsMapping, batch: &RecordBatch) -> Vec<NormalizedPoint> {
        mapping.validate().expect("the mapping is valid");
        let limits = IngestLimits::default();
        let cols = MetricsColumnIndex::resolve(batch, mapping).expect("columns resolve");
        let (_, is_monotonic_sum) = mapping.metric_kind();
        let mut grouper = mapping
            .is_histogram()
            .then(|| HistogramGrouper::new(tenant()));
        let (mut points, rows) = build_batch_points(
            batch,
            &cols,
            0,
            &tenant(),
            mapping,
            &limits,
            &TenantRules::none(),
            NOW_NS,
            is_monotonic_sum,
            grouper.as_mut(),
        )
        .expect("every row is admitted");
        if let Some(grouper) = grouper.as_mut() {
            let closed = grouper.finish(&limits).expect("the last group closes");
            points.extend(closed.points);
            assert_eq!(
                rows + closed.rows,
                batch.num_rows() as u64,
                "every source row is accounted to some closed group"
            );
        }
        points
    }

    fn sorted_ids(points: &[NormalizedPoint]) -> Vec<SeriesId> {
        let mut ids: Vec<SeriesId> = points.iter().map(|p| p.series_id).collect();
        ids.sort_unstable();
        ids
    }

    fn scalar_mapping(kind: Option<MetricKindArg>) -> MetricsMapping {
        MetricsMapping {
            name: Some(METRIC.to_string()),
            name_column: None,
            value_column: "value".to_string(),
            ts_column: "ts".to_string(),
            ts_unit: TsUnit::Nanos,
            unit: Some(UNIT.to_string()),
            kind,
            labels: vec![LabelMap {
                name: ATTR_KEY.to_string(),
                column: "method".to_string(),
            }],
            histogram: None,
        }
    }

    fn scalar_batch(value: f64) -> RecordBatch {
        batch(vec![
            ("ts", i64_col(vec![EVENT_NS])),
            (
                "value",
                Arc::new(Float64Array::from(vec![value])) as ArrayRef,
            ),
            ("method", str_col(vec![ATTR_VALUE])),
        ])
    }

    fn number_point() -> NumberDataPoint {
        NumberDataPoint {
            attributes: attributes(),
            time_unix_nano: EVENT_NS as u64,
            value: Some(NumberValue::AsDouble(1.5)),
            ..Default::default()
        }
    }

    /// The `__name__` label of one point, which is also the family name
    /// `SeriesId::compute` was keyed on.
    fn metric_name_of(point: &NormalizedPoint) -> String {
        point
            .labels
            .iter()
            .find(|l| l.name == METRIC_NAME_LABEL)
            .map(|l| l.value.clone())
            .expect("every point carries __name__")
    }

    fn label_names(point: &NormalizedPoint) -> Vec<String> {
        point.labels.iter().map(|l| l.name.clone()).collect()
    }

    #[test]
    fn a_gauge_lands_on_the_otlp_series_id() {
        let points = loader_points(&scalar_mapping(None), &scalar_batch(1.5));
        assert_eq!(points.len(), 1);
        assert_eq!(
            metric_name_of(&points[0]),
            "http_server_duration_seconds",
            "the dotted name is sanitized and the unit suffix applied"
        );
        assert!(
            label_names(&points[0]).contains(&"http_method".to_string()),
            "the dotted attribute key is sanitized: {:?}",
            label_names(&points[0])
        );
        assert!(
            !points[0].is_monotonic_sum,
            "a gauge is not a monotonic sum"
        );
        assert_eq!(
            sorted_ids(&points),
            otlp_series_ids(MetricData::Gauge(Gauge {
                data_points: vec![number_point()],
            })),
            "a gauge loaded from Parquet is the same series as the same OTLP gauge"
        );
    }

    #[test]
    fn a_counter_gains_total_and_lands_on_the_otlp_series_id() {
        let points = loader_points(
            &scalar_mapping(Some(MetricKindArg::Counter)),
            &scalar_batch(1.5),
        );
        assert_eq!(points.len(), 1);
        assert_eq!(
            metric_name_of(&points[0]),
            "http_server_duration_seconds_total",
            "kind = \"counter\" adds _total exactly as a monotonic OTLP Sum does"
        );
        assert!(
            points[0].is_monotonic_sum,
            "kind = \"counter\" sets is_monotonic_sum, which a gauge leaves false"
        );
        assert_eq!(
            sorted_ids(&points),
            otlp_series_ids(MetricData::Sum(Sum {
                data_points: vec![number_point()],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            "a counter loaded from Parquet is the same series as a monotonic OTLP Sum"
        );
    }

    #[test]
    fn a_classic_histogram_lands_on_the_otlp_series_ids() {
        let mapping = MetricsMapping {
            name: Some(METRIC.to_string()),
            name_column: None,
            value_column: "bucket_count".to_string(),
            ts_column: "ts".to_string(),
            ts_unit: TsUnit::Nanos,
            unit: Some(UNIT.to_string()),
            kind: None,
            labels: vec![LabelMap {
                name: ATTR_KEY.to_string(),
                column: "method".to_string(),
            }],
            histogram: Some(HistogramMap {
                histogram_type: None,
                le_column: "le".to_string(),
                sum_column: "sum".to_string(),
                count_column: "count".to_string(),
            }),
        };
        // Two explicit bounds, each row carrying that bucket's own count,
        // and the data point's sum and count repeated on both rows.
        let rows = batch(vec![
            ("ts", i64_col(vec![EVENT_NS, EVENT_NS])),
            (
                "le",
                Arc::new(Float64Array::from(vec![0.1, 1.0])) as ArrayRef,
            ),
            (
                "bucket_count",
                Arc::new(Float64Array::from(vec![2.0, 3.0])) as ArrayRef,
            ),
            (
                "sum",
                Arc::new(Float64Array::from(vec![12.5, 12.5])) as ArrayRef,
            ),
            ("count", i64_col(vec![7, 7])),
            ("method", str_col(vec![ATTR_VALUE, ATTR_VALUE])),
        ]);
        let points = loader_points(&mapping, &rows);
        assert_eq!(
            points.len(),
            5,
            "two bounds explode into 2 buckets + the +Inf bucket + _sum + _count"
        );
        for point in &points {
            assert!(
                metric_name_of(point).starts_with("http_server_duration_seconds_"),
                "every exploded name carries the sanitized, unit-suffixed family name: {}",
                metric_name_of(point)
            );
            assert!(
                !metric_name_of(point).contains("_total"),
                "no exploded histogram series is a monotonic sum: {}",
                metric_name_of(point)
            );
            assert!(!point.is_monotonic_sum);
        }
        // OTLP's bucket_counts is one longer than explicit_bounds: the last
        // element is the +Inf bucket's own count.
        assert_eq!(
            sorted_ids(&points),
            otlp_series_ids(MetricData::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: attributes(),
                    time_unix_nano: EVENT_NS as u64,
                    count: 7,
                    sum: Some(12.5),
                    bucket_counts: vec![2, 3, 2],
                    explicit_bounds: vec![0.1, 1.0],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            "every exploded series of a loaded histogram matches the OTLP explosion"
        );
    }

    /// The empty-label-value rule, at the identity level: a row whose
    /// label cell is empty is the same series as one whose cell is null,
    /// and both are the series OTLP produces for a data point with no
    /// such attribute at all.
    #[test]
    fn an_empty_label_value_is_dropped_like_a_missing_attribute() {
        let mapping = scalar_mapping(None);
        let empty = batch(vec![
            ("ts", i64_col(vec![EVENT_NS])),
            ("value", Arc::new(Float64Array::from(vec![1.5])) as ArrayRef),
            ("method", str_col(vec![""])),
        ]);
        let null = batch(vec![
            ("ts", i64_col(vec![EVENT_NS])),
            ("value", Arc::new(Float64Array::from(vec![1.5])) as ArrayRef),
            (
                "method",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);
        let from_empty = loader_points(&mapping, &empty);
        let from_null = loader_points(&mapping, &null);
        assert_eq!(
            sorted_ids(&from_empty),
            sorted_ids(&from_null),
            "an empty label cell and a null one are one series, not two"
        );
        assert!(
            !label_names(&from_empty[0]).contains(&"http_method".to_string()),
            "the empty label is absent from the series: {:?}",
            label_names(&from_empty[0])
        );
        assert_eq!(
            sorted_ids(&from_empty),
            otlp_series_ids(MetricData::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: ATTR_KEY.to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueVariant::StringValue(String::new())),
                        }),
                        ..Default::default()
                    }],
                    time_unix_nano: EVENT_NS as u64,
                    value: Some(NumberValue::AsDouble(1.5)),
                    ..Default::default()
                }],
            })),
            "OTLP drops an empty attribute value before the label set is built"
        );
    }
}

/// A count column wider than `f64`'s exact integer range keeps its value.
/// Reading it through `f64` moved it to the nearest representable value,
/// which is a silently wrong count, not a rejection.
#[test]
fn a_count_above_two_to_the_53_survives_an_integer_column() {
    let big: u64 = (1u64 << 53) + 1;
    let arr: ArrayRef = Arc::new(UInt64Array::from(vec![big]));
    assert_eq!(
        read_count(&arr, 0).expect("a u64 column is a valid count column"),
        Some(big),
        "the count must not round through f64"
    );
}

/// A float count of exactly 2^64 is one past `u64::MAX` and used to pass
/// the `> u64::MAX as f64` test (that cast rounds UP to 2^64), then
/// saturate to `u64::MAX` on the way in.
#[test]
fn a_float_count_of_exactly_two_to_the_64_is_refused() {
    let two_to_64 = 18_446_744_073_709_551_616.0f64;
    let err = exact_count(two_to_64).expect_err("2^64 does not fit in u64");
    assert!(
        err.contains("fits in u64"),
        "the refusal says what is wrong: {err}"
    );
    // One representable step below still passes, so the bound is at 2^64
    // and not merely "large floats are refused".
    let below = 18_446_744_073_709_549_568.0f64;
    assert!(below < two_to_64);
    assert_eq!(exact_count(below).expect("below 2^64 fits"), below as u64);
}

/// Fix-round regressions for the metrics submit loop and the histogram
/// grouper (PR #2096 review).
mod metrics_pipeline_review {
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const DRAIN_MAPPING: &str = "[metrics]\nname = \"drain_probe\"\nvalue_column = \
                                     \"value\"\nts_column = \"ts\"\nts_unit = \"nanos\"\nkind = \
                                     \"gauge\"\n\n[[metrics.label]]\nname = \"host\"\ncolumn = \
                                     \"host\"\n";

    fn drain_mapping() -> MetricsMapping {
        match parse_mapping_document(DRAIN_MAPPING, SignalArg::Metrics).expect("valid mapping") {
            MappingSection::Metrics(m) => m,
            _ => panic!("a [metrics] section parses as metrics"),
        }
    }

    fn drain_batch(hosts: &[&str]) -> RecordBatch {
        let n = hosts.len();
        batch(vec![
            ("ts", i64_col(vec![NOW_NS - 60_000_000_000; n])),
            (
                "value",
                Arc::new(Float64Array::from(vec![1.0; n])) as ArrayRef,
            ),
            ("host", str_col(hosts.to_vec())),
        ])
    }

    /// A `host` label value whose series routes to each of `shards` shards,
    /// found through the loader's own point builder so the routing is the
    /// one `IngestRouter::write` applies.
    pub(super) fn host_per_shard(mapping: &MetricsMapping, shards: u32) -> Vec<String> {
        let candidates: Vec<String> = (0..256).map(|i| format!("h{i}")).collect();
        let refs: Vec<&str> = candidates.iter().map(String::as_str).collect();
        let b = drain_batch(&refs);
        let cols = MetricsColumnIndex::resolve(&b, mapping).expect("columns resolve");
        let (points, _) = build_batch_points(
            &b,
            &cols,
            0,
            &TenantId::new("acme"),
            mapping,
            &IngestLimits::default(),
            &TenantRules::none(),
            NOW_NS,
            false,
            None,
        )
        .expect("every candidate row is admitted");
        (0..shards)
            .map(|shard| {
                let idx = points
                    .iter()
                    .position(|p| ravel_types::shard_for(&p.series_id, shards) == shard)
                    .expect("some candidate routes to every shard");
                candidates[idx].clone()
            })
            .collect()
    }

    /// Every data-object key under one metrics shard.
    pub(super) async fn shard_data_keys(store: &dyn ObjectStoreBackend, shard: u32) -> Vec<String> {
        let needle = format!("/m/l0/{shard:04}/");
        let mut out = Vec::new();
        let mut page: Option<ravel_object_store::PageToken> = None;
        loop {
            let p = store.list("", page).await.expect("list");
            out.extend(
                p.objects
                    .into_iter()
                    .map(|o| o.key)
                    .filter(|k| k.contains(&needle)),
            );
            match p.next {
                Some(t) => page = Some(t),
                None => break,
            }
        }
        out
    }

    /// A write that fails in the FINAL drain, with a later outstanding
    /// write that succeeds, reports that later write's token in the
    /// returned error's durable list.
    ///
    /// Two one-row batches at `--pipeline-depth 3` never fill the window,
    /// so both writes are still outstanding when the loop ends and both
    /// resolve in the end-of-load drain, oldest first. Batch 0 routes to
    /// shard 0, whose data PUT fails permanently; batch 1 routes to shard
    /// 1 and commits. The durable list must be exactly batch 1's one token,
    /// and that token must name the object that actually landed on shard 1.
    ///
    /// Non-vacuity: against the drain that kept the first error and
    /// resolved the rest into the report only, this fails on the
    /// `durable.len()` assertion with 0 tokens, because the error's list
    /// was cloned before batch 1 resolved.
    #[tokio::test]
    async fn a_final_drain_failure_keeps_a_later_writes_tokens() {
        use parquet::arrow::ArrowWriter;

        let shards = 2;
        let mapping = drain_mapping();
        let hosts = host_per_shard(&mapping, shards);

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("drain.parquet");
        let b = drain_batch(&[hosts[0].as_str(), hosts[1].as_str()]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
            )
            .with_key_contains("/m/l0/0000/")
            .with_occurrence(Occurrence::Always),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));

        let err = load_metrics(
            fault.clone() as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &mapping,
            shards,
            1,
            0,
            3,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            1,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("batch 0's permanent PUT failure fails the load");
        assert!(
            fault.fault_count(Op::Put, FaultKind::Permanent) >= 1,
            "the scripted fault must have fired"
        );
        let LoadError::Flush { durable, .. } = &err else {
            panic!("expected LoadError::Flush, got {err:?}");
        };
        assert_eq!(
            durable.len(),
            1,
            "exactly batch 1's one token is durable, and it resolved after the failure: \
                 {durable:?}"
        );
        let token = &durable[0];
        assert_eq!(token.shard, 1, "the durable token is batch 1's shard");
        let landed = shard_data_keys(fault.inner(), 1).await;
        let prefix = format!(
            "/m/l0/0001/{}.{}.{:020}.",
            token.writer_id, token.epoch, token.seq
        );
        assert_eq!(landed.len(), 1, "one object landed on shard 1: {landed:?}");
        assert!(
            landed[0].contains(&prefix),
            "the reported token names the object that landed ({prefix} in {landed:?})"
        );
        assert!(
            shard_data_keys(fault.inner(), 0).await.is_empty(),
            "nothing landed on the failing shard"
        );
    }

    /// A row rejection found while an earlier write is failing keeps the
    /// rejection (its row and reason) and carries the drain's durable
    /// list, including a later write that committed, rather than being
    /// replaced by the write's `Flush` error.
    ///
    /// Three one-row batches at `--pipeline-depth 3`: batch 0 routes to
    /// shard 0 and its PUT fails, batch 1 routes to shard 1 and commits,
    /// and row 2 carries a far-future timestamp the decoder rejects, so
    /// the rejection drains both writes first.
    ///
    /// Non-vacuity: against the `drain_sequential_inflight(..).await?` call
    /// in the `Rejected` arm, the load returns the `Flush` error and this
    /// fails on the `RowRejected` match.
    #[tokio::test]
    async fn a_row_rejection_after_a_failed_write_keeps_its_reason_and_the_drained_tokens() {
        use parquet::arrow::ArrowWriter;

        let shards = 2;
        let mapping = drain_mapping();
        let hosts = host_per_shard(&mapping, shards);

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("reject.parquet");
        let b = batch(vec![
            (
                "ts",
                i64_col(vec![
                    NOW_NS - 60_000_000_000,
                    NOW_NS - 60_000_000_000,
                    NOW_NS + 86_400_000_000_000,
                ]),
            ),
            (
                "value",
                Arc::new(Float64Array::from(vec![1.0, 1.0, 1.0])) as ArrayRef,
            ),
            (
                "host",
                str_col(vec![
                    hosts[0].as_str(),
                    hosts[1].as_str(),
                    hosts[1].as_str(),
                ]),
            ),
        ]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
            )
            .with_key_contains("/m/l0/0000/")
            .with_occurrence(Occurrence::Always),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));

        let err = load_metrics(
            fault.clone() as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &mapping,
            shards,
            1,
            0,
            3,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            1,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("row 2 is rejected");
        assert!(
            fault.fault_count(Op::Put, FaultKind::Permanent) >= 1,
            "the scripted fault must have fired"
        );
        let LoadError::RowRejected {
            row,
            reason,
            durable,
            ..
        } = &err
        else {
            panic!("expected the row rejection to survive the drain, got {err:?}");
        };
        assert_eq!(*row, 2, "the rejected row is the far-future one");
        assert!(
            reason.contains("an earlier write had also failed: flush failed:"),
            "the write failure is named beside the rejection: {reason}"
        );
        assert_eq!(durable.len(), 1, "batch 1's one token: {durable:?}");
        let token = &durable[0];
        let prefix = format!(
            "/m/l0/0001/{}.{}.{:020}.",
            token.writer_id, token.epoch, token.seq
        );
        let landed = shard_data_keys(fault.inner(), 1).await;
        assert_eq!(landed.len(), 1, "one object landed on shard 1: {landed:?}");
        assert!(
            landed[0].contains(&prefix),
            "the reported token names the object that landed ({prefix} in {landed:?})"
        );
    }

    /// A two-row metrics file whose second row is far in the future, and
    /// the mapping file beside it, for the CLI-level tests below.
    fn rejecting_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("metrics.parquet");
        let b = batch(vec![
            (
                "ts",
                i64_col(vec![NOW_NS - 60_000_000_000, NOW_NS + 86_400_000_000_000]),
            ),
            (
                "value",
                Arc::new(Float64Array::from(vec![1.0, 1.0])) as ArrayRef,
            ),
            ("host", str_col(vec!["a", "a"])),
        ]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");
        let mapping_path = dir.path().join("mapping.toml");
        std::fs::write(&mapping_path, DRAIN_MAPPING).expect("write mapping");
        (dir, pq, mapping_path)
    }

    async fn run_metrics_cli(
        pq: &Path,
        mapping_path: &Path,
        read_cursors: Option<usize>,
        pipeline_depth: usize,
        decode_queue_batches: usize,
    ) -> (anyhow::Result<()>, String) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut sink: Vec<u8> = Vec::new();
        let outcome = run_warning_to(
            store,
            pq,
            "acme",
            mapping_path,
            SignalArg::Metrics,
            1,
            1,
            0,
            read_cursors,
            Some(pipeline_depth),
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            decode_queue_batches,
            Some(PER_BATCH_TARGET_BYTES),
            None,
            RlogZstdLevel::DEFAULT,
            None,
            NOW_NS,
            &mut sink,
        )
        .await;
        (
            outcome,
            String::from_utf8(sink).expect("warnings are utf-8"),
        )
    }

    /// A failed metrics load above depth 1 blames the pipeline depth only
    /// and names `--pipeline-depth 1` as the remedy, never the
    /// `--read-cursors` flag the metrics path ignores.
    ///
    /// Non-vacuity: against `resume_hint(&err, Some(1), pipeline_depth)` in
    /// `run_metrics`, the emitted-verdict assertion fails, the stream
    /// carrying "this run used --read-cursors 1 and --pipeline-depth 2".
    #[tokio::test]
    async fn a_failed_metrics_load_names_only_the_pipeline_depth() {
        let (_dir, pq, mapping_path) = rejecting_fixture();
        let (outcome, emitted) =
            run_metrics_cli(&pq, &mapping_path, None, 2, DEFAULT_DECODE_QUEUE_BATCHES).await;
        let err = outcome.expect_err("row 1 is far in the future and is rejected");
        let load_err = err
            .downcast::<LoadError>()
            .expect("the CLI error wraps the typed load error");
        assert!(
            matches!(load_err, LoadError::RowRejected { row: 1, .. }),
            "{load_err:?}"
        );

        let verdict = "this run used --pipeline-depth 2, so the rows that landed are NOT a \
                           contiguous prefix of the file: a batch submitted after the failing \
                           one can still have committed. Resuming at this offset would both \
                           re-ingest committed rows and skip rows that never landed. Only a load \
                           started with --pipeline-depth 1 is resumable this way.";
        assert!(
            emitted.contains(verdict),
            "the metrics verdict is emitted: {emitted}"
        );
        assert!(
            emitted.contains(METRICS_ADMISSION_BYPASS_WARNING),
            "the metrics admission warning is the one printed: {emitted}"
        );
        assert!(
            !emitted.contains("--read-cursors"),
            "a metrics failure never names --read-cursors: {emitted}"
        );

        let hint = sequential_resume_hint(&load_err, Some(2)).expect("a row rejection has figures");
        assert_eq!(
            hint,
            format!(
                "resume figures for this failed load:\n  \
                     rows_skipped     : 0\n  \
                     rows_written     : 1\n  \
                     next --skip-rows : 1 (rows_skipped + rows_written)\n\
                     {verdict} There is no deduplication and no per-file idempotency marker, so \
                     nothing checks the offset a re-run is given; see docs/guides/ingest.md for \
                     the procedure."
            )
        );
    }

    /// `--read-cursors 0` and `--decode-queue-batches 0` are rejected on a
    /// metrics load with the logs path's messages, before the warning that
    /// the metrics path ignores them.
    ///
    /// Non-vacuity: without the two guards in `run_metrics` the load runs
    /// and fails on the fixture's far-future row instead, so the message
    /// assertion fails with "row 1: timestamp is ... ahead of load time".
    #[tokio::test]
    async fn zero_read_cursors_or_decode_queue_is_rejected_on_a_metrics_load() {
        let (_dir, pq, mapping_path) = rejecting_fixture();
        for (read_cursors, decode_queue, message) in [
            (Some(0), DEFAULT_DECODE_QUEUE_BATCHES, READ_CURSORS_ZERO),
            (None, 0, DECODE_QUEUE_BATCHES_ZERO),
        ] {
            let (outcome, emitted) =
                run_metrics_cli(&pq, &mapping_path, read_cursors, 1, decode_queue).await;
            let err = outcome.expect_err("a zero lever is rejected");
            assert_eq!(err.to_string(), message);
            assert!(
                !emitted.contains("a metrics load ignores"),
                "the rejection comes before the unused-lever warning: {emitted}"
            );
        }
        assert!(READ_CURSORS_ZERO.starts_with("--read-cursors must be at least 1"));
        assert!(DECODE_QUEUE_BATCHES_ZERO.starts_with("--decode-queue-batches must be at least 1"));
    }

    fn bucket_row(le: f64) -> MetricRow {
        MetricRow {
            name: "latency".to_string(),
            labels: Vec::new(),
            ts_ns: NOW_NS,
            payload: RowPayload::Bucket(BucketRow {
                le,
                own_count: 1,
                sum: Some(10.0),
                count: 10,
            }),
        }
    }

    /// The bucket limit is enforced while a group accumulates, not only
    /// when it closes: a ten-row data point under a limit of 4 is refused
    /// on its fifth bucket row, with the close-time message and the
    /// group's first row, and the open group never holds more than 4
    /// bounds.
    ///
    /// Non-vacuity: with the check only at close time, the fifth push
    /// returns `Ok` and the test fails on the `held <= 4` assertion with
    /// "the open group holds 5 bounds after row 4".
    #[test]
    fn the_bucket_limit_refuses_an_open_group_at_the_first_row_past_it() {
        let limits = IngestLimits {
            max_histogram_buckets: 4,
            ..IngestLimits::default()
        };
        let first_row = 100;
        let mut grouper = HistogramGrouper::new(TenantId::new("acme"));
        let mut refused = None;
        for i in 0..10u64 {
            let outcome = grouper.push(bucket_row(i as f64 + 1.0), first_row + i, &limits);
            let held = grouper.pending.as_ref().map_or(0, |g| g.bounds.len());
            assert!(
                held <= 4,
                "the open group holds {held} bounds after row {i}"
            );
            if let Err(e) = outcome {
                refused = Some((i, e));
                break;
            }
        }
        let Some((index, (row, message))) = refused else {
            panic!("a ten-row group over a limit of 4 must be refused");
        };
        assert_eq!(index, 4, "the fifth bucket row is refused");
        assert_eq!(
            row, first_row,
            "the refusal points at the group's first row"
        );
        assert_eq!(
            message,
            format!(
                "the \"latency\" data point at ts {NOW_NS} has 5 explicit bounds, more than \
                     the limit of 4"
            ),
        );
    }
}

/// Issue #2753: the derived object geometry on the metrics path.
mod default_geometry {
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const MAPPING: &str = "[metrics]\nname = \"geometry_probe\"\nvalue_column = \"value\"\n\
                           ts_column = \"ts\"\nts_unit = \"nanos\"\nkind = \"gauge\"\n\n\
                           [[metrics.label]]\nname = \"host\"\ncolumn = \"host\"\n";

    const BATCH_ROWS: usize = 10_000;
    const BATCHES: usize = 51;
    const HOST_BYTES: u64 = 64 << 30;

    fn mapping() -> MetricsMapping {
        match parse_mapping_document(MAPPING, SignalArg::Metrics).expect("valid mapping") {
            MappingSection::Metrics(m) => m,
            _ => panic!("a [metrics] section parses as metrics"),
        }
    }

    /// `batches` row groups of `batch_rows` points. Each row group is one
    /// series of its own (a fixed-width `host` value), with a distinct
    /// timestamp per point: the points estimate charges a series' bytes once
    /// per buffer, at its first sighting there, so a distinct series per batch
    /// keeps every batch's estimate equal and a buffer's estimate the sum of
    /// its batches'.
    fn fixture(batch_rows: usize, batches: usize) -> (tempfile::TempDir, std::path::PathBuf) {
        hosted_fixture(batch_rows, batches, |g| format!("b{g:03}"))
    }

    /// [`fixture`] with row group `g` on the series `host(g)`.
    fn hosted_fixture(
        batch_rows: usize,
        batches: usize,
        host: impl Fn(usize) -> String,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        row_hosted_fixture(batch_rows, batches, |g, _| host(g))
    }

    /// [`fixture`] with row `r` of row group `g` on the series `host(g, r)`.
    fn row_hosted_fixture(
        batch_rows: usize,
        batches: usize,
        host: impl Fn(usize, usize) -> String,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("points.parquet");
        let group = |g: usize| {
            let base = NOW_NS - 60_000_000_000 + (g * batch_rows) as i64;
            let hosts: Vec<String> = (0..batch_rows).map(|r| host(g, r)).collect();
            batch(vec![
                (
                    "ts",
                    i64_col((0..batch_rows).map(|r| base + r as i64).collect()),
                ),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![1.0; batch_rows])) as ArrayRef,
                ),
                ("host", str_col(hosts.iter().map(String::as_str).collect())),
            ])
        };
        let first = group(0);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
        writer.write(&first).expect("write row group");
        writer.flush().expect("flush row group");
        for g in 1..batches {
            writer.write(&group(g)).expect("write row group");
            writer.flush().expect("flush row group");
        }
        writer.close().expect("close writer");
        (dir, pq)
    }

    async fn load(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        request: GeometryRequest,
    ) -> MetricsLoadReport {
        load_sharded(store, pq, request, 1, BATCH_ROWS).await
    }

    async fn load_sharded(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        request: GeometryRequest,
        shards: u32,
        batch_rows: usize,
    ) -> MetricsLoadReport {
        try_load_sharded(store, pq, request, shards, batch_rows)
            .await
            .expect("load succeeds")
    }

    async fn try_load_sharded(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        request: GeometryRequest,
        shards: u32,
        batch_rows: usize,
    ) -> Result<MetricsLoadReport, LoadError> {
        try_load_clocked(
            store,
            pq,
            request,
            shards,
            batch_rows,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
    }

    async fn try_load_clocked(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        request: GeometryRequest,
        shards: u32,
        batch_rows: usize,
        clock: Arc<dyn Clock>,
    ) -> Result<MetricsLoadReport, LoadError> {
        load_metrics_with(
            store,
            pq,
            "acme",
            &mapping(),
            shards,
            batch_rows,
            0,
            request,
            None,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            Some(DEFAULT_MAX_FLUSH_DELAY),
            NOW_NS,
            clock,
            Arc::new(TenantRules::none()),
        )
        .await
    }

    fn host_derived() -> LoadMemoryRequest {
        LoadMemoryRequest::Derived {
            host_total_bytes: Some(HOST_BYTES),
        }
    }

    /// Data objects on shard 0.
    async fn stored_objects(store: &dyn ObjectStoreBackend) -> usize {
        let mut stored = 0;
        let mut page: Option<ravel_object_store::PageToken> = None;
        loop {
            let p = store.list("", page).await.expect("list");
            stored += p
                .objects
                .iter()
                .filter(|o| o.key.contains("/m/l0/0000/"))
                .count();
            match p.next {
                Some(t) => page = Some(t),
                None => break,
            }
        }
        stored
    }

    /// Issue #2753: one series across every batch makes the first batch
    /// over-estimate the rest, since a buffer charges a series' bytes only at
    /// its first sighting there. 25 batches then estimate below the derived
    /// target (25 first batches' worth), the size trigger never fires, and
    /// the depth flush closes each window of 25: `51 / 25` depth flushes and
    /// one end-of-input flush for the 1-batch remainder.
    ///
    /// Prove-the-test: never call `window.submitted` in the metrics loader
    /// and nothing answers the write the loop waits on at the 25th submit;
    /// the pinned clock never fires the age trigger, so the load fails on the
    /// Strict ack deadline and `load`'s `expect("load succeeds")` panics.
    #[tokio::test]
    async fn an_over_estimating_first_batch_is_closed_by_the_depth_flush() {
        let (_dir, pq) = hosted_fixture(BATCH_ROWS, BATCHES, |_| "b000".to_string());
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load(
            Arc::clone(&store),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: host_derived(),
            },
        )
        .await;

        assert_eq!(report.points_written, (BATCH_ROWS * BATCHES) as u64);
        let layout = report.layout_report();
        let geometry = layout.geometry.expect("geometry resolved");
        assert_eq!(geometry.target_source, TargetSource::Derived);
        let depth = geometry.pipeline_depth;
        assert_eq!(depth, 25);
        let depth_flushes = (BATCHES / depth) as u64;
        let manual = u64::from(!BATCHES.is_multiple_of(depth));
        assert_eq!(
            stored_objects(store.as_ref()).await as u64,
            depth_flushes + manual
        );
        assert_eq!(
            layout.flush_mix.totals,
            FlushMixCounts {
                size: 0,
                age: 0,
                arrival: 0,
                depth: depth_flushes,
                manual,
            },
            "no size flush is reachable, so the depth flush closes every full window"
        );
    }

    /// With `--target-bytes` and `--pipeline-depth` unset, 51 batches of
    /// 10,000 points on one shard make exactly ceil(510,000 / 250,000) = 3
    /// objects: every 25th batch reaches the derived target (250,000 times
    /// the first batch's estimate per row) and closes an object on size, and
    /// the end-of-input drain publishes the 10,000-point tail. The derived
    /// depth is ceil(250,000 / 10,000) = 25; the size flushes at batches 25
    /// and 50 each restart the depth window, so the depth flush never comes
    /// due.
    ///
    /// The clock is pinned, so the age trigger cannot fire. Per-batch
    /// flushing writes 51 objects; a target the size trigger never reaches
    /// leaves every object to the depth flush or the drain (size 0).
    #[tokio::test]
    async fn default_load_merges_batches_into_target_sized_objects() {
        let (_dir, pq) = fixture(BATCH_ROWS, BATCHES);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load(
            Arc::clone(&store),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: host_derived(),
            },
        )
        .await;

        assert_eq!(report.points_written, (BATCH_ROWS * BATCHES) as u64);
        let expected_objects = (BATCH_ROWS * BATCHES).div_ceil(DEFAULT_OBJECT_ROWS);
        assert_eq!(expected_objects, 3);
        assert_eq!(
            stored_objects(store.as_ref()).await,
            expected_objects,
            "510,000 points on one shard make ceil(510,000 / 250,000) objects"
        );
        assert_eq!(report.objects_written(), expected_objects);

        let layout = report.layout_report();
        let geometry = layout
            .geometry
            .expect("a load that decoded rows resolves its geometry");
        assert_eq!(geometry.target_source, TargetSource::Derived);
        // `est_bytes_per_row` is the first batch's estimate per row rounded
        // up; the target is 250,000 rows of the unrounded figure.
        let rows = DEFAULT_OBJECT_ROWS as u64;
        assert!(
            (geometry.target_bytes as u64) <= rows * geometry.est_bytes_per_row
                && (geometry.target_bytes as u64) > rows * (geometry.est_bytes_per_row - 1),
            "the derived target is 250,000 rows of the first batch's estimate: {geometry:?}"
        );
        assert_eq!(geometry.rows_depth, 25);
        assert_eq!(geometry.pipeline_depth, 25);
        assert_eq!(geometry.depth_bound, DepthBound::Rows);
        assert_eq!(
            layout.flush_mix.totals,
            FlushMixCounts {
                size: 2,
                age: 0,
                arrival: 0,
                depth: 0,
                manual: 1,
            },
            "two full objects close on size, the tail on the end-of-input drain"
        );
        assert_eq!(layout.flush_mix.totals.dominant(), Some("size"));
        assert_eq!(layout.target_bytes_no_effect_warning, None);
    }

    const HISTOGRAM_MAPPING: &str = "[metrics]\nname = \"geometry_histogram\"\nvalue_column \
                                     = \"bucket_count\"\nts_column = \"ts\"\nts_unit = \
                                     \"nanos\"\n\n[[metrics.label]]\nname = \"host\"\ncolumn = \
                                     \"host\"\n\n[metrics.histogram]\nle_column = \
                                     \"le\"\nsum_column = \"sum\"\ncount_column = \"count\"\n";

    /// The finite bounds of every fixture data point: one source row each.
    const BOUNDS: [f64; 2] = [0.1, 1.0];

    /// One data point of `BOUNDS.len()` finite bounds and a null sum explodes
    /// into one bucket per bound, the `+Inf` bucket and `_count`.
    const POINTS_PER_DATA_POINT: usize = BOUNDS.len() + 2;

    fn histogram_mapping() -> MetricsMapping {
        match parse_mapping_document(HISTOGRAM_MAPPING, SignalArg::Metrics).expect("valid mapping")
        {
            MappingSection::Metrics(m) => m,
            _ => panic!("a [metrics] section parses as metrics"),
        }
    }

    /// Row group `g` of the histogram fixture: `batch_rows / BOUNDS.len()`
    /// data points on the series of host `b{g:03}`, one source row per bound,
    /// each data point at its own `ts`.
    fn histogram_group(g: usize, batch_rows: usize) -> RecordBatch {
        let per_group = batch_rows / BOUNDS.len();
        let base = NOW_NS - 60_000_000_000 + (g * per_group) as i64;
        let host = format!("b{g:03}");
        batch(vec![
            (
                "ts",
                i64_col(
                    (0..batch_rows)
                        .map(|r| base + (r / BOUNDS.len()) as i64)
                        .collect(),
                ),
            ),
            (
                "le",
                Arc::new(Float64Array::from(
                    (0..batch_rows)
                        .map(|r| BOUNDS[r % BOUNDS.len()])
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "bucket_count",
                Arc::new(Float64Array::from(vec![1.0; batch_rows])) as ArrayRef,
            ),
            (
                "sum",
                Arc::new(Float64Array::from(vec![None::<f64>; batch_rows])) as ArrayRef,
            ),
            ("count", i64_col(vec![BOUNDS.len() as i64; batch_rows])),
            ("host", str_col(vec![host.as_str(); batch_rows])),
        ])
    }

    fn histogram_fixture(
        batch_rows: usize,
        batches: usize,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("histograms.parquet");
        let first = histogram_group(0, batch_rows);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
        writer.write(&first).expect("write row group");
        writer.flush().expect("flush row group");
        for g in 1..batches {
            writer
                .write(&histogram_group(g, batch_rows))
                .expect("write row group");
            writer.flush().expect("flush row group");
        }
        writer.close().expect("close writer");
        (dir, pq)
    }

    /// Each shard-0 data object's sample count, read from its RSEG footer.
    async fn object_points(store: &dyn ObjectStoreBackend) -> Vec<u64> {
        let mut out = Vec::new();
        for key in super::metrics_pipeline_review::shard_data_keys(store, 0).await {
            let got = store
                .get(&key, ravel_object_store::GetRange::Full)
                .await
                .expect("get object");
            let location = ravel_segment::open_from_full(got.data.as_ref(), Default::default())
                .expect("open rseg footer");
            out.push(location.footer.sample_count);
        }
        out.sort_unstable();
        out
    }

    /// Issue #2753: the derived target counts points, the object's own rows,
    /// not source rows. A classic-histogram data point of 2 finite bounds and
    /// a null sum is 2 source rows and 4 points, so a batch of 10,000 rows
    /// carries 20,000 points. 27 batches make 540,000 points on one shard:
    /// size flushes after 13 batches (259,996 points: the first batch leaves
    /// its last data point open for the second) and 26 (260,000), and the
    /// end-of-input drain publishes the last 20,004 (the last batch's 20,000
    /// and the data point the end of input closes).
    ///
    /// Prove-the-test: resolve the geometry on the first batch's source rows
    /// (9,998) instead of its points (19,996) and the target doubles to
    /// 250,000 x 2 points' worth, so the first object holds about 500,000
    /// points and the target assertion fails first.
    #[tokio::test]
    async fn a_histogram_load_sizes_objects_in_points_not_source_rows() {
        const BATCHES: usize = 27;
        let (_dir, pq) = histogram_fixture(BATCH_ROWS, BATCHES);
        let mapping = histogram_mapping();

        // The loader's first write, built independently: every data point of
        // row group 0 but the last, which stays open until row group 1.
        let first = histogram_group(0, BATCH_ROWS);
        let cols = MetricsColumnIndex::resolve(&first, &mapping).expect("columns resolve");
        let tenant = TenantId::new("acme");
        let mut grouper = HistogramGrouper::new(tenant.clone());
        let (first_points, first_rows) = build_batch_points(
            &first,
            &cols,
            0,
            &tenant,
            &mapping,
            &IngestLimits::default(),
            &TenantRules::none(),
            NOW_NS,
            false,
            Some(&mut grouper),
        )
        .expect("every row is admitted");
        let data_points = BATCH_ROWS / BOUNDS.len();
        assert_eq!(first_rows as usize, BATCH_ROWS - BOUNDS.len());
        assert_eq!(
            first_points.len(),
            (data_points - 1) * POINTS_PER_DATA_POINT,
            "the first write carries n + 2 points per n source rows"
        );
        let first_points: Vec<ravel_ingest::IngestPoint> = first_points
            .into_iter()
            .map(ravel_ingest::IngestPoint::from)
            .collect();
        let est = ravel_ingest::est_points_object_bytes(&first_points);

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load_metrics_with(
            Arc::clone(&store),
            &pq,
            "acme",
            &mapping,
            1,
            BATCH_ROWS,
            0,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: host_derived(),
            },
            None,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            Some(DEFAULT_MAX_FLUSH_DELAY),
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            Arc::new(TenantRules::none()),
        )
        .await
        .expect("load succeeds");

        let total_points = BATCHES * data_points * POINTS_PER_DATA_POINT;
        assert_eq!(report.rows_processed, (BATCHES * BATCH_ROWS) as u64);
        assert_eq!(report.points_written, total_points as u64);

        let layout = report.layout_report();
        let geometry = layout.geometry.expect("geometry resolved");
        assert_eq!(geometry.target_source, TargetSource::Derived);
        assert_eq!(
            geometry.target_bytes,
            DEFAULT_OBJECT_ROWS * est / first_points.len(),
            "the derived target is 250,000 points at the first write's estimate per point"
        );
        assert_eq!(
            geometry.est_bytes_per_row,
            est.div_ceil(first_points.len()) as u64
        );

        let objects = object_points(store.as_ref()).await;
        assert_eq!(objects.iter().sum::<u64>(), total_points as u64);
        assert_eq!(
            objects.len(),
            total_points.div_ceil(DEFAULT_OBJECT_ROWS),
            "540,000 points on one shard make ceil(540,000 / 250,000) objects: {objects:?}"
        );
        let batch_points = (data_points * POINTS_PER_DATA_POINT) as u64;
        let (tail, full) = objects.split_first().expect("objects were written");
        assert_eq!(
            *tail,
            batch_points + POINTS_PER_DATA_POINT as u64,
            "the last batch and the data point closed at end of input are the drained tail"
        );
        for &points in full {
            assert!(
                points >= DEFAULT_OBJECT_ROWS as u64
                    && points < DEFAULT_OBJECT_ROWS as u64 + batch_points,
                "a size-closed object holds 250,000 points plus less than one batch: \
                 {objects:?}"
            );
        }
        assert_eq!(
            layout.flush_mix.totals,
            FlushMixCounts {
                size: 2,
                age: 0,
                arrival: 0,
                depth: 0,
                manual: 1,
            }
        );
    }

    /// `--target-bytes 1 --batch-rows 10000` writes one object per batch on
    /// the one shard, each closed by the size trigger.
    #[tokio::test]
    async fn target_bytes_one_writes_one_object_per_batch_per_shard() {
        let batches = 3;
        let (_dir, pq) = fixture(BATCH_ROWS, batches);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load(
            Arc::clone(&store),
            &pq,
            GeometryRequest {
                target_bytes: Some(PER_BATCH_TARGET_BYTES),
                pipeline_depth: None,
                load_memory: host_derived(),
            },
        )
        .await;
        assert_eq!(report.objects_written(), batches);
        assert_eq!(
            report.layout_report().flush_mix.totals,
            FlushMixCounts {
                size: batches as u64,
                age: 0,
                arrival: 0,
                depth: 0,
                manual: 0,
            }
        );
    }

    /// A budget of three batches at the first batch's estimate decides the
    /// derived depth over the rows bound's 25, and the summary says so.
    #[tokio::test]
    async fn a_small_memory_budget_decides_the_derived_depth() {
        let (_dir, pq) = fixture(BATCH_ROWS, 2);
        let probe = load(
            Arc::new(MemoryStore::new()),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: host_derived(),
            },
        )
        .await;
        let est_bytes_per_row = probe.geometry.expect("geometry resolved").est_bytes_per_row;
        let report = load(
            Arc::new(MemoryStore::new()),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: LoadMemoryRequest::Flag(3 * BATCH_ROWS as u64 * est_bytes_per_row),
            },
        )
        .await;
        let geometry = report.geometry.expect("geometry resolved");
        assert_eq!(geometry.memory_depth, Some(3));
        assert_eq!(geometry.pipeline_depth, 3);
        assert_eq!(geometry.depth_bound, DepthBound::Memory);
        let summary = render_layout_summary(&report.layout_report());
        assert!(
            summary.contains(
                "pipeline depth   : 3 (memory bound decided it: memory bound 3, rows bound 25)"
            ),
            "{summary}"
        );
    }

    /// The metrics summary carries the trigger mix and the no-effect warning,
    /// both as text and in the JSON line, which parses back to the report.
    /// `--target-bytes 2` is below one batch's content, so all three writes
    /// flush on size as their own object and the warning applies.
    #[tokio::test]
    async fn the_summary_and_json_carry_the_trigger_mix_and_the_warning() {
        let (_dir, pq) = fixture(BATCH_ROWS, 3);
        let report = load(
            Arc::new(MemoryStore::new()),
            &pq,
            GeometryRequest {
                target_bytes: Some(2),
                pipeline_depth: None,
                load_memory: host_derived(),
            },
        )
        .await;
        let layout = report.layout_report();
        let warning = layout
            .target_bytes_no_effect_warning
            .clone()
            .expect("a target below one batch changes nothing and says so");
        assert!(warning.contains("--target-bytes 2"), "{warning}");

        let summary = render_layout_summary(&layout);
        assert!(
            summary.contains(
                "  flush triggers   : size 3, age 0, arrival 0, depth 0, manual 0 (total 3)"
            ),
            "{summary}"
        );
        assert!(summary.contains(&warning), "{summary}");
        let json = summary
            .lines()
            .find_map(|l| l.strip_prefix(LOAD_REPORT_JSON_PREFIX))
            .expect("the summary ends with the JSON report");
        let value: serde_json::Value = serde_json::from_str(json).expect("the line is JSON");
        assert_eq!(value["signal"], "metrics");
        assert_eq!(value["flush_mix"]["totals"]["size"], 3);
        assert_eq!(value["flush_mix"]["totals"]["manual"], 0);
        assert_eq!(value["target_bytes_no_effect_warning"], warning.as_str());
        let parsed: LoadLayoutReport = serde_json::from_str(json).expect("the report round-trips");
        assert_eq!(parsed, layout);
    }

    /// Issue #2753: a size flush restarts the depth window of its own shard
    /// only. Four shards, `--pipeline-depth 8`, batches of 40,000 points:
    /// batches 1-6 put every point on series `a` (shard A), batch 7 splits
    /// 20,000 / 20,000 between `a` and series `b` (shard B), and batches 8-12
    /// put every point on `b`. The derived target is 250,000 of the first
    /// batch's points (one series charge per 40,000 points), so `a` reaches
    /// it at batch 7 with 260,000 points and closes on size; `b` ends with
    /// 220,000 points, below the target. Shard A's window restarts at batch
    /// 7; shard B has been pending since batch 7, and a depth flush would
    /// come due at batch 7 + 8 - 1 = 14, after the last batch, so the
    /// end-of-input flush closes `b`.
    ///
    /// Prove-the-test: under the router-wide rule (a size flush clears
    /// pending shards only when the router-wide size-flush count grew by the
    /// number of shards the batch reached), batch 7 reached two shards and
    /// opened one size flush, so nothing is cleared, shard A's batch-1 entry
    /// comes due at batch 8, and the depth flush closes `b`'s batches 7-8 as
    /// an object of its own: 3 objects, and the mix assertion fails with
    /// size 1, depth 1, manual 1.
    #[tokio::test]
    async fn a_size_flush_restarts_only_that_shards_window() {
        const SHARDS: u32 = 4;
        const ROWS: usize = 40_000;
        const BATCHES: usize = 12;
        const DEPTH: usize = 8;
        let hosts = super::metrics_pipeline_review::host_per_shard(&mapping(), SHARDS);
        let (shard_a, shard_b) = (0u32, 1u32);
        let (a, b) = (hosts[0].clone(), hosts[1].clone());
        let (_dir, pq) = row_hosted_fixture(ROWS, BATCHES, |g, r| match g {
            0..=5 => a.clone(),
            6 if r < ROWS / 2 => a.clone(),
            _ => b.clone(),
        });
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load_sharded(
            Arc::clone(&store),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: Some(DEPTH),
                load_memory: host_derived(),
            },
            SHARDS,
            ROWS,
        )
        .await;

        assert_eq!(report.points_written, (ROWS * BATCHES) as u64);
        let layout = report.layout_report();
        let geometry = layout.geometry.expect("geometry resolved");
        assert_eq!(geometry.target_source, TargetSource::Derived);
        assert_eq!(geometry.pipeline_depth, DEPTH);

        // Points per series, and the batch each series is first pending in
        // (1-based, as the depth window counts submits).
        let a_points = 6 * ROWS + ROWS / 2;
        let b_points = ROWS / 2 + (BATCHES - 7) * ROWS;
        assert!(a_points >= DEFAULT_OBJECT_ROWS && b_points < DEFAULT_OBJECT_ROWS);
        let b_first_pending = 7;
        assert!(
            b_first_pending + DEPTH - 1 > BATCHES,
            "b's window never comes due"
        );

        let mix = layout.flush_mix.totals;
        assert_eq!(
            mix,
            FlushMixCounts {
                size: 1,
                age: 0,
                arrival: 0,
                depth: 0,
                manual: 1,
            },
            "a closes on size at batch 7; b's window, started at batch 7, is not due by batch 12"
        );
        let shard_keys = super::metrics_pipeline_review::shard_data_keys;
        assert_eq!(shard_keys(store.as_ref(), shard_a).await.len(), 1);
        assert_eq!(shard_keys(store.as_ref(), shard_b).await.len(), 1);
        assert_eq!(report.objects_written() as u64, mix.total());
        assert_eq!(report.realign_gave_up, 0);
    }

    /// Issue #2753: a shard that never handles its writes makes the load give
    /// up realigning once, on the injected clock, and say so in the report,
    /// the summary line and the JSON line. The skew override holds every
    /// shard one write short of its enqueued count, so the first submit's
    /// settle waits until the test advances the clock by the settle timeout;
    /// no wall-clock time passes. Once given up, the later submits never
    /// settle again (the override is read exactly once more, by the round
    /// that sees the deadline), and the load still writes every point.
    #[tokio::test]
    async fn a_shard_that_never_merges_gives_up_realigning_on_the_clock() {
        use std::sync::atomic::{AtomicU64, Ordering};

        use crate::load::geometry::{SETTLE_TIMEOUT, SKEW_OVERRIDE};

        const BATCHES: usize = 3;
        let (_dir, pq) = fixture(BATCH_ROWS, BATCHES);
        let reads = Arc::new(AtomicU64::new(0));
        let override_reads = Arc::clone(&reads);
        let stall: crate::load::geometry::SkewOverride = Arc::new(move |stats| {
            override_reads.fetch_add(1, Ordering::SeqCst);
            stats
                .into_iter()
                .map(|(shard, mut s)| {
                    s.messages_enqueued = s.messages_processed + 1;
                    (shard, s)
                })
                .collect()
        });
        let clock = TestClock::new(NOW_NS);
        let run = SKEW_OVERRIDE.scope(
            stall,
            try_load_clocked(
                Arc::new(MemoryStore::new()),
                &pq,
                GeometryRequest {
                    target_bytes: None,
                    pipeline_depth: None,
                    load_memory: host_derived(),
                },
                1,
                BATCH_ROWS,
                clock.clone(),
            ),
        );
        tokio::pin!(run);
        // The settle loop yields 64 rounds before it sleeps on the clock, so
        // more reads than that mean its deadline is set and it is parked.
        while reads.load(Ordering::SeqCst) < 64 {
            tokio::select! {
                biased;
                _ = &mut run => panic!("the load finished while a shard held its writes"),
                () = yield_until_router_is_quiet(&clock) => {}
            }
        }
        let parked = reads.load(Ordering::SeqCst);
        clock.advance_ns(i64::try_from(SETTLE_TIMEOUT.as_nanos()).expect("fits"));
        let report = loop {
            tokio::select! {
                biased;
                done = &mut run => break done.expect("load succeeds"),
                () = yield_until_router_is_quiet(&clock) => assert!(
                    reads.load(Ordering::SeqCst) <= parked + 1,
                    "a later submit settled again after the window gave up"
                ),
            }
        };
        assert_eq!(reads.load(Ordering::SeqCst), parked + 1);

        assert_eq!(report.realign_gave_up, 1);
        assert_eq!(report.points_written, (BATCHES * BATCH_ROWS) as u64);
        let layout = report.layout_report();
        let summary = render_layout_summary(&layout);
        assert!(
            summary.contains(
                "  depth realign    : gave up 1 time(s): the shards had not handled a batch \
                 within 30 s, so later depth flushes counted batches without size flushes and \
                 may have closed objects early"
            ),
            "{summary}"
        );
        let json = summary
            .lines()
            .find_map(|l| l.strip_prefix(LOAD_REPORT_JSON_PREFIX))
            .expect("the summary ends with the JSON report");
        let value: serde_json::Value = serde_json::from_str(json).expect("the line is JSON");
        assert_eq!(value["realign_gave_up"], 1);
        let parsed: LoadLayoutReport = serde_json::from_str(json).expect("parses back");
        assert_eq!(parsed.realign_gave_up, 1);

        let settled = load(
            Arc::new(MemoryStore::new()),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: host_derived(),
            },
        )
        .await;
        assert_eq!(settled.realign_gave_up, 0);
        let summary = render_layout_summary(&settled.layout_report());
        assert!(
            summary.contains("  depth realign    : per shard on size flushes\n"),
            "{summary}"
        );
        let json = summary
            .lines()
            .find_map(|l| l.strip_prefix(LOAD_REPORT_JSON_PREFIX))
            .expect("the summary ends with the JSON report");
        let value: serde_json::Value = serde_json::from_str(json).expect("the line is JSON");
        assert_eq!(value["realign_gave_up"], 0);
    }

    /// Issue #2753: a decode refusal at the default geometry reports at once.
    /// Three 2-point batches on one shard, the third opening with a point a
    /// day in the future, which the decoder rejects. The derived target is
    /// far above the two buffered batches and the pinned clock never fires
    /// the age trigger, so only the refusal path's own flush
    /// (`drain_sequential_before_refusal`) can answer their acks: the load
    /// returns the row rejection with both earlier batches durable as one
    /// object.
    ///
    /// Prove-the-test: delete the `flush_buffered(router, enqueue).await` in
    /// `drain_sequential_before_refusal` and nothing answers the buffered
    /// acks until the Strict ack deadline expires on wall time; the drain
    /// then fails, the rejection carries that failure's empty durable list,
    /// and the `durable.len()` assertion fails with 0.
    #[tokio::test]
    async fn a_refused_batch_flushes_the_buffered_writes_before_it_reports() {
        let host = "b000";
        let mut ts: Vec<i64> = (0..6).map(|r| NOW_NS - 60_000_000_000 + r).collect();
        ts[4] = NOW_NS + 86_400_000_000_000;
        let (_dir, pq) = write_parquet(&batch(vec![
            ("ts", i64_col(ts)),
            (
                "value",
                Arc::new(Float64Array::from(vec![1.0; 6])) as ArrayRef,
            ),
            ("host", str_col(vec![host; 6])),
        ]));
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = try_load_sharded(
            Arc::clone(&store),
            &pq,
            GeometryRequest {
                target_bytes: None,
                pipeline_depth: None,
                load_memory: host_derived(),
            },
            1,
            2,
        )
        .await
        .expect_err("row 4 is a day in the future");

        let LoadError::RowRejected { row, durable, .. } = &err else {
            panic!("expected the row rejection, got {err:?}");
        };
        assert_eq!(*row, 4);
        assert_eq!(durable.len(), 2, "both earlier batches acked: {durable:?}");
        assert_eq!(
            stored_objects(store.as_ref()).await,
            1,
            "the refusal flush published the two buffered batches as one object"
        );
    }
}
