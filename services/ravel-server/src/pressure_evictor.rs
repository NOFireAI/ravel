//! The one pressure evictor the server registers on its process memory
//! budget (ADR-3326 decision 4, pressure-evictor amendment). A reservation
//! the budget refuses, from any consumer and for either exhaustion cause,
//! first asks the decoded column-block cache (when one is attached) for the
//! shortfall plus its admission margin, then asks the catalog's decoded
//! parts and postings caches for whatever the first did not free.

use std::sync::{Arc, OnceLock, Weak};

use ravel_catalog::Catalog;
use ravel_logseg::decoded_cache::DecodedColumnCache;
use ravel_memory::{ExhaustionCause, MemoryBudget};

/// The caches a refused reservation on the process budget evicts from. The
/// budget's registered closure holds the only strong handle beyond the one
/// [`PressureEvictor::install`] returns; both caches are held weakly.
pub(crate) struct PressureEvictor {
    catalog: Weak<Catalog>,
    decoded: OnceLock<(Weak<DecodedColumnCache>, u64)>,
}

impl PressureEvictor {
    /// Registers the evictor on `budget` with slack 0, so the catalog is
    /// asked for the bare shortfall and the decoded cache's margin is added
    /// only when that cache is attached. Errs when `budget` already has an
    /// evictor.
    pub(crate) fn install(
        budget: &MemoryBudget,
        catalog: Weak<Catalog>,
    ) -> anyhow::Result<Arc<PressureEvictor>> {
        let evictor = Arc::new(PressureEvictor {
            catalog,
            decoded: OnceLock::new(),
        });
        let registered = Arc::clone(&evictor);
        if budget
            .register_evictor(
                Box::new(move |want, cause| registered.evict(want, cause)),
                0,
            )
            .is_err()
        {
            anyhow::bail!("process memory budget already has an evictor");
        }
        Ok(evictor)
    }

    /// Puts the decoded column-block cache ahead of the catalog: a refusal
    /// asks it for the shortfall plus `margin` first. Errs on a second
    /// attach.
    #[cfg_attr(not(feature = "sql"), allow(dead_code))]
    pub(crate) fn attach_decoded_cache(
        &self,
        cache: &Arc<DecodedColumnCache>,
        margin: u64,
    ) -> anyhow::Result<()> {
        if self.decoded.set((Arc::downgrade(cache), margin)).is_err() {
            anyhow::bail!(
                "a decoded column-block cache is already attached to the pressure evictor"
            );
        }
        Ok(())
    }

    fn evict(&self, want: u64, cause: ExhaustionCause) -> u64 {
        let from_decoded = self
            .decoded
            .get()
            .and_then(|(cache, margin)| {
                cache.upgrade().map(|cache| {
                    cache.evict_for_pressure(want.saturating_add(*margin), *margin, cause)
                })
            })
            .unwrap_or(0);
        let rest = want.saturating_sub(from_decoded);
        let from_catalog = if rest == 0 {
            0
        } else {
            self.catalog
                .upgrade()
                .map_or(0, |catalog| catalog.evict_decoded_for_pressure(rest))
        };
        from_decoded.saturating_add(from_catalog)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use ravel_catalog::{
        CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
        DEFAULT_MAX_FLUSH_LIFETIME_NS,
    };
    use ravel_commit::keys;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_logseg::decoded_cache::{DecodedCacheAttach, DecodedCacheCounters};
    use ravel_logseg::{
        AttrValue, ColumnSelection, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader,
        RlogWriter,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::accounting::AccessIntent;
    use ravel_types::{Signal, TenantHash, TimeRange};
    use uuid::Uuid;

    const LIMIT: u64 = 4 << 30;
    const NS_PER_HOUR: i64 = 3_600 * 1_000_000_000;
    const SEALED_HOUR: u32 = 500_000;

    fn fold_now_ns() -> i64 {
        (i64::from(SEALED_HOUR) + 1) * NS_PER_HOUR
            + DEFAULT_MAX_FLUSH_LIFETIME_NS
            + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
            + DEFAULT_FOLD_SAFETY_MARGIN_NS
    }

    fn config() -> CatalogConfig {
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        }
    }

    /// Publishes `segments` metric commit records for `tenant` in
    /// [`SEALED_HOUR`] and folds them into one snapshot part.
    async fn fold_one_part(store: &Arc<MemoryStore>, tenant: TenantHash, segments: u64) {
        let created = (i64::from(SEALED_HOUR) + 1) * NS_PER_HOUR - 1_000;
        for seq in 1..=segments {
            let writer_id = Uuid::new_v4();
            let payload = format!("segment-{seq}-{writer_id}").into_bytes();
            let rec = record::build(NewCommitRecord {
                tenant_hash: tenant,
                signal: Signal::Metrics,
                shard: 0,
                writer_id,
                writer_epoch: 1,
                writer_seq: seq,
                object_size: payload.len() as u64,
                content_hash: *blake3::hash(&payload).as_bytes(),
                sample_count: 1,
                series_count: 1,
                min_event_ts_ns: created - 1_000,
                max_event_ts_ns: created,
                min_ingest_ts_ns: created - 1_000,
                max_ingest_ts_ns: created,
                segment_format_version: 1,
                created_unix_ns: created,
                ingest_hour_bucket: SEALED_HOUR,
            })
            .expect("valid record");
            let data_key = keys::reconstruct_data_key(&rec).expect("data key");
            publish::put_data_object(store.as_ref(), &data_key, Bytes::from(payload))
                .await
                .expect("put data object");
            publish::publish(store.as_ref(), &rec, &RetryPolicy::default())
                .await
                .expect("publish");
        }
        Catalog::new(store.clone(), config())
            .expect("fold catalog")
            .fold(
                &tenant,
                Signal::Metrics,
                Uuid::new_v4(),
                fold_now_ns(),
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot HEAD");
    }

    /// A catalog charging `budget`, its decoded caches holding one snapshot
    /// part for each of two tenants.
    async fn filled_catalog(budget: &Arc<MemoryBudget>) -> Arc<Catalog> {
        let store = Arc::new(MemoryStore::new());
        let tenants = [TenantHash([0x41; 16]), TenantHash([0x42; 16])];
        for (i, tenant) in tenants.iter().enumerate() {
            fold_one_part(&store, *tenant, i as u64 + 2).await;
        }
        let catalog = Arc::new(
            Catalog::new(store, config())
                .expect("catalog")
                .with_memory_budget(budget.clone()),
        );
        let range = TimeRange {
            start_ns: i64::from(SEALED_HOUR) * NS_PER_HOUR,
            end_ns: (i64::from(SEALED_HOUR) + 1) * NS_PER_HOUR - 1,
        };
        for tenant in &tenants {
            catalog
                .resolve(tenant, Signal::Metrics, range, &[], fold_now_ns())
                .await
                .expect("resolve fills the part cache");
        }
        assert!(catalog.decoded_part_cache_reserved_bytes() > 0);
        catalog
    }

    fn catalog_reserved(catalog: &Catalog) -> u64 {
        catalog.decoded_part_cache_reserved_bytes()
            + catalog.decoded_postings_cache_reserved_bytes()
    }

    /// A decoded column-block cache charging `budget`, filled by one columnar
    /// scan of a small log object that has since been dropped, so no entry is
    /// held.
    fn filled_decoded_cache(budget: &Arc<MemoryBudget>) -> Arc<DecodedColumnCache> {
        let cfg = RlogConfig {
            block_target_records: 4,
            ..RlogConfig::default()
        };
        let res = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [7; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for i in 0..16i64 {
            writer
                .push(LogRecord {
                    stream_id: ravel_types::logstream::log_stream_id(&res, "scope", "1", &[]),
                    stream_attrs: ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]),
                    ts_ns: 100 + i,
                    observed_ts_ns: 100 + i,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("body number {i}"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push log record");
        }
        let obj = writer.finish().expect("finish rlog object");
        let cache = Arc::new(DecodedColumnCache::new(1 << 30, Some(budget.clone())));
        let reader = RlogReader::new(&obj, &cfg).expect("open object");
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("open cursor");
        scan.attach_decoded_cache(DecodedCacheAttach {
            cache: Arc::clone(&cache),
            tenant: [7; 16],
            object: [9; 32],
            intent: AccessIntent::Interactive,
            counters: Arc::new(DecodedCacheCounters::default()),
            serve_deferred: false,
        });
        while scan
            .next_block_columnar(obj.as_slice())
            .expect("columnar exit")
            .is_some()
        {}
        drop(scan);
        assert!(cache.resident_bytes() > 0, "the scan admitted entries");
        cache
    }

    /// With no decoded column cache attached, a reservation the budget
    /// refuses evicts the catalog's least-recently-used decoded entry and is
    /// admitted. Without the evictor the same reservation is refused.
    #[tokio::test]
    async fn with_no_decoded_cache_a_refused_reservation_evicts_the_catalog_cache() {
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let catalog = filled_catalog(&budget).await;
        let held = budget.reserved();
        assert_eq!(held, catalog_reserved(&catalog));

        let over = LIMIT - held + 1;
        assert!(
            budget.try_reserve(over).is_err(),
            "one byte over the limit is refused with no evictor"
        );
        assert_eq!(catalog.decoded_cache_memory_evictions(), 0);

        let _evictor =
            PressureEvictor::install(&budget, Arc::downgrade(&catalog)).expect("install");
        budget
            .try_reserve(over)
            .expect("admitted once the catalog gives an entry back");
        assert_eq!(
            catalog.decoded_cache_memory_evictions(),
            1,
            "a one-byte shortfall takes one entry"
        );
        assert_eq!(budget.reserved(), over + catalog_reserved(&catalog));
        assert!(catalog_reserved(&catalog) < held);
        budget.release(over);
    }

    /// A host memory gate refusal runs the same evictor: the catalog gives
    /// back at least the gate's shortfall and the retry is admitted.
    #[tokio::test]
    async fn a_host_memory_refusal_evicts_the_catalog_cache() {
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let catalog = filled_catalog(&budget).await;
        let held = budget.reserved();
        budget.set_host_memory_gate(false, 0, 1);
        assert!(budget.try_reserve(1).is_err(), "the closed gate refuses");

        let _evictor =
            PressureEvictor::install(&budget, Arc::downgrade(&catalog)).expect("install");
        budget
            .try_reserve(1)
            .expect("admitted once the catalog frees the gate's shortfall");
        assert_eq!(catalog.decoded_cache_memory_evictions(), 1);
        assert_eq!(budget.reserved(), 1 + catalog_reserved(&catalog));
        assert!(catalog_reserved(&catalog) < held);
        budget.release(1);
    }

    /// With a decoded column cache attached that covers the shortfall plus
    /// its margin on its own, the refusal is met from that cache and the
    /// catalog's caches are not touched.
    #[tokio::test]
    async fn an_attached_decoded_cache_is_asked_before_the_catalog() {
        let budget = Arc::new(MemoryBudget::new(LIMIT));
        let catalog = filled_catalog(&budget).await;
        let cache = filled_decoded_cache(&budget);
        let catalog_held = catalog_reserved(&catalog);
        let resident = cache.resident_bytes();
        assert_eq!(budget.reserved(), catalog_held + resident);
        let margin = resident / 2;

        let over = LIMIT - budget.reserved() + 1;
        assert!(budget.try_reserve(over).is_err());

        let evictor = PressureEvictor::install(&budget, Arc::downgrade(&catalog)).expect("install");
        evictor
            .attach_decoded_cache(&cache, margin)
            .expect("first attach");
        let before = cache.pressure_counters();
        budget
            .try_reserve(over)
            .expect("admitted from the decoded cache");
        let moved = cache.pressure_counters().since(&before);
        assert!(moved.evictions >= 1, "{moved:?}");
        assert!(
            moved.freed_bytes > margin,
            "asked for the shortfall plus the margin: {moved:?}"
        );
        assert_eq!(
            catalog.decoded_cache_memory_evictions(),
            0,
            "the catalog is asked only for what the decoded cache did not free"
        );
        assert_eq!(catalog_reserved(&catalog), catalog_held);
        assert_eq!(
            budget.reserved(),
            over + catalog_held + cache.resident_bytes()
        );
        budget.release(over);

        assert!(
            evictor.attach_decoded_cache(&cache, margin).is_err(),
            "a second attach is refused"
        );
    }

    #[test]
    fn a_second_install_on_one_budget_is_refused() {
        let budget = MemoryBudget::new(LIMIT);
        let _first = PressureEvictor::install(&budget, Weak::new()).expect("first install");
        assert!(PressureEvictor::install(&budget, Weak::new()).is_err());
    }
}
