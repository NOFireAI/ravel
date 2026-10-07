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
                let r = build_metric_row(b, &cols, &mapping, &limits, NOW_NS, row)
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
        let out = normalize_metrics(&tenant(), request(data), &IngestLimits::default(), NOW_NS);
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
    fn host_per_shard(mapping: &MetricsMapping, shards: u32) -> Vec<String> {
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
    async fn shard_data_keys(store: &dyn ObjectStoreBackend, shard: u32) -> Vec<String> {
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
            pipeline_depth,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            decode_queue_batches,
            DEFAULT_TARGET_BYTES,
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

        let hint = sequential_resume_hint(&load_err, 2).expect("a row rejection has figures");
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
