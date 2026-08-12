//! Catalog: snapshot resolution over commit records (MVCC).
//!
//! Implementer contract: docs/catalog-and-mvcc.md and ADR-0003. Phase 1 is
//! listing-based discovery behind the Catalog API; snapshots come with
//! compaction.

mod auth_token_map;
mod cache;
mod catalog;
mod config;
mod covering_postings;
mod error;
mod fold;
mod key_epoch;
mod provisioning;
mod seal_divergence;
mod snapshot;
mod snapshot_format;
mod snapshot_resolve;
mod tenant_config;

pub use auth_token_map::{
    AUTH_KEY, AUTH_TOKEN_MAP_FORMAT_VERSION, AuthMapDefect, AuthTokenMap, AuthTokenMapError,
    KEY_FINGERPRINT_LEN, SetOutcome as AuthSetOutcome, TOKEN_HASH_LEN, TokenEntry, key_fingerprint,
    read_auth_map, remove_token, remove_tokens_by_tenant, replace_tenant_tokens, tenant_for_token,
    token_hash, upsert_token,
};
pub use catalog::Catalog;
pub use config::{
    CatalogConfig, DEFAULT_BYTE_CACHE_MAX_BYTES, DEFAULT_BYTE_CACHE_MAX_ENTRIES,
    DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES, DEFAULT_CACHE_CAPACITY_PER_TENANT,
    DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_HEAD_CACHE_CAPACITY,
    DEFAULT_HEAD_CACHE_TTL_NS, DEFAULT_MAX_CATALOG_LIST_REQUESTS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    DEFAULT_MAX_INGEST_LAG_NS, DEFAULT_POSTINGS_CACHE_ENTRIES,
    DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS, DEFAULT_SNAPSHOT_CACHE_PARTS,
};
pub use covering_postings::{LoadPostingsError, LoadedCoveringPostings, load_covering_postings};
pub use error::CatalogError;
pub use fold::{FoldReport, PostingsBuildError, Transaction, fetch_segment_names};
pub use key_epoch::{
    EpochDefect, KEY_EPOCH_FORMAT_VERSION, KeyEpoch, KeyEpochError, KeyEpochOutcome, enc_key,
    epoch_for_write, read_epochs, read_epochs_checked, read_epochs_from_store, record_key_epoch,
};
pub use provisioning::{
    AbsentPolicy, DEFAULT_SCAN_SLACK_HOURS, FloorDefect, FloorRaiseOutcome, FormatFloor,
    GenerationDefect, MAX_SHARD_COUNT, PROVISIONING_FORMAT_VERSION, ProvisioningCheck,
    ProvisioningError, ReshardOutcome, ShardGeneration, active_shard_count, append_generation,
    current_floor, current_floor_from_store, max_scan_count_over_range, provisioning_key,
    raise_format_floor, read_floors, read_floors_checked, read_floors_from_store, read_generations,
    read_generations_checked, read_generations_from_store, scan_count, shard_ceiling,
    validate_or_adopt,
};
pub use seal_divergence::{
    EntryIdentity, SealDivergenceError, SealDivergenceReport, verify_seal_divergence,
};
pub use snapshot::{SegmentLevel, SegmentRef, Snapshot};
pub use snapshot_format::{
    DEFAULT_MAX_POSTINGS_BYTES, DEFAULT_MAX_SNAPSHOT_PART_BYTES, DecodedPart, DecodedPostings,
    HEAD_FORMAT_VERSION, MAGIC, NamePostings, PartLimits, PostingsLimits, SnapshotFormatError,
    VERSION, decode_head, decode_part, decode_postings, encode_head, encode_part, encode_postings,
};
pub use tenant_config::{
    SetOutcome as TenantConfigSetOutcome, TENANT_CONFIG_FORMAT_VERSION, TenantConfig,
    TenantConfigError, TenantLifecycleState, config_key, read_config, read_config_values,
    set_tenant_config,
};
