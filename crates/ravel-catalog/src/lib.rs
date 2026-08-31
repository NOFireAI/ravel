//! Catalog: snapshot resolution over commit records (MVCC).
//!
//! Implementer contract: docs/catalog-and-mvcc.md and ADR-0003. Phase 1 is
//! listing-based discovery behind the Catalog API; snapshots come with
//! compaction.

mod auth_token_map;
mod cache;
mod catalog;
mod column_stats_build;
mod column_stats_resolve;
mod config;
mod covering_postings;
mod declared_stats;
mod error;
mod fold;
mod key_epoch;
mod metrics_meta;
mod provisioning;
mod seal_divergence;
mod snapshot;
mod snapshot_format;
mod snapshot_resolve;
mod tenant_config;

pub use auth_token_map::{
    AUTH_KEY, AUTH_TOKEN_MAP_FORMAT_VERSION, AuthMapDefect, AuthTokenMap, AuthTokenMapError,
    KEY_FINGERPRINT_LEN, MANAGED_BY_CLI, MANAGED_BY_OPERATOR, SetOutcome as AuthSetOutcome,
    TOKEN_HASH_LEN, TokenEntry, key_fingerprint, read_auth_map, remove_token,
    remove_tokens_by_tenant, remove_tokens_by_tenant_owned_by, replace_tenant_tokens,
    tenant_for_token, token_hash, upsert_token, upsert_token_owned,
};
pub use catalog::Catalog;
pub use catalog::resolve_rewrite_supersession;
pub use column_stats_resolve::{LoadColumnStatsError, LoadedColumnStats, unique_column_stat};
pub use config::{
    CatalogConfig, DEFAULT_BYTE_CACHE_MAX_BYTES, DEFAULT_BYTE_CACHE_MAX_ENTRIES,
    DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES, DEFAULT_CACHE_CAPACITY_PER_TENANT,
    DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_COLUMN_STATS_CACHE_MAX_BYTES,
    DEFAULT_FOLD_RECONCILE_WINDOW_HOURS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_FRONTIER_RECONCILE_MAX_HOURS, DEFAULT_HEAD_CACHE_CAPACITY, DEFAULT_HEAD_CACHE_TTL_NS,
    DEFAULT_MAX_CATALOG_LIST_REQUESTS, DEFAULT_MAX_FLUSH_LIFETIME_NS, DEFAULT_MAX_INGEST_LAG_NS,
    DEFAULT_POSTINGS_CACHE_ENTRIES, DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS,
    DEFAULT_PROTECTION_HORIZON_NS, DEFAULT_SNAPSHOT_CACHE_PARTS,
};
pub use covering_postings::{LoadPostingsError, LoadedCoveringPostings, load_covering_postings};
pub use declared_stats::{DeclaredColumnStats, read_snapshot_entry};
pub use error::CatalogError;
pub use fold::{FoldReport, PostingsBuildError, Transaction, fetch_segment_names};
pub use key_epoch::{
    EpochDefect, KEY_EPOCH_FORMAT_VERSION, KeyEpoch, KeyEpochError, KeyEpochOutcome, enc_key,
    epoch_for_write, read_epochs, read_epochs_checked, read_epochs_from_store, record_key_epoch,
};
pub use metrics_meta::{
    DEFAULT_METRICS_META_ENTRY_CAP, MAX_METRICS_META_DECOMPRESSED_BYTES,
    METRICS_META_FORMAT_VERSION, MergeOutcome, MetricKind, MetricMetadataEntry, MetricsMetaDefect,
    MetricsMetaError, merge_entries, metrics_meta_key, read_metrics_meta, write_metrics_meta,
};
pub use provisioning::{
    AbsentPolicy, DEFAULT_SCAN_SLACK_HOURS, FLUSH_BOUND_SLACK_HOURS, FloorDefect,
    FloorRaiseOutcome, FormatFloor, GenerationDefect, MAX_SHARD_COUNT, PROVISIONING_FORMAT_VERSION,
    ProvisioningCheck, ProvisioningError, ReshardOutcome, ShardGeneration, active_shard_count,
    append_generation, current_floor, current_floor_from_store, max_scan_count_over_range,
    provisioning_key, raise_format_floor, read_floors, read_floors_checked, read_floors_from_store,
    read_generations, read_generations_checked, read_generations_from_store, scan_count,
    shard_ceiling, stable_generation_for_hour, validate_or_adopt,
};
pub use seal_divergence::{
    EntryIdentity, SealDivergenceError, SealDivergenceReport, verify_seal_divergence,
};
pub use snapshot::{SegmentLevel, SegmentOrigin, SegmentOrigins, SegmentRef, Snapshot};
pub use snapshot_format::{
    ColumnStatsLimits, DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES, DEFAULT_MAX_COLUMN_STATS_BYTES,
    DEFAULT_MAX_POSTINGS_BYTES, DEFAULT_MAX_SNAPSHOT_PART_BYTES, DecodedColumnStats, DecodedPart,
    DecodedPostings, HEAD_FORMAT_VERSION, MAGIC, NamePostings, PartLimits, PostingsLimits,
    SnapshotFormatError, VERSION, decode_column_stats, decode_head, decode_part, decode_postings,
    encode_column_stats, encode_head, encode_part, encode_postings, validate_min_max_presence,
};
pub use tenant_config::{
    DeclaredColumnType, DeclaredTypedColumn, FIXED_LOGS_SQL_COLUMNS,
    SetOutcome as TenantConfigSetOutcome, TENANT_CONFIG_FORMAT_VERSION, TenantConfig,
    TenantConfigError, TenantLifecycleState, TypedAttrColumnError, config_key, read_config,
    read_config_values, set_tenant_config, validate_typed_attr_columns,
};
