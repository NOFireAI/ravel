//! Location grants (ADR-2040 decision D1): the locations an operator has
//! admitted for one tenant, and the checks that decide whether a URL or a raw
//! object key falls inside one.
//!
//! The record lives at `t/<tenant_hash>/pq/grants`, one per tenant, and is
//! rewritten whole under CAS. It is deliberately not a field of the tenant
//! config record, which the ingest and fold paths read: a grant change must
//! not invalidate their cached config, and this record has its own reader
//! floor ([`PARQUET_GRANTS_MIN_READ_VERSION`]).
//!
//! A location is identified by (profile, bucket, key), never by its URL
//! string: an `az://` URL carries no account, and an `s3://` bucket name is
//! unique only per endpoint. The profile supplies that missing half.
//!
//! Containment is segment-wise in both directions a caller needs it:
//! [`resolve_location`] decides whether a LOCATION URL falls inside a grant,
//! and [`contains_key`] decides whether a raw object key does. A grant of
//! prefix `data` admits `data/x.parquet` and `data/`; it never admits
//! `data2/x.parquet`. An empty prefix grants the whole bucket. Keys are
//! compared as bytes and are case-sensitive.

use bytes::Bytes;
use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::parquet_table::v1 as pb;
use ravel_types::TenantHash;

use crate::clock::Clock;
use crate::keys::{KeyError, grants_key, parse_grants_key};
use crate::names::GLOB_CHARS;

/// Format floor written into every grants record this build emits.
pub const PARQUET_GRANTS_FORMAT_VERSION: u32 = 1;

/// Lowest `format_version` this build reads. Version 0 is an unstamped
/// record, which no supported writer produces.
pub const PARQUET_GRANTS_MIN_READ_VERSION: u32 = 1;

/// Highest `format_version` this build reads. Equal to the writer stamp: the
/// record has had no additive change.
pub const PARQUET_GRANTS_MAX_READ_VERSION: u32 = PARQUET_GRANTS_FORMAT_VERSION;

/// How many times [`add`] and [`remove`] re-read and retry their CAS put
/// before giving up.
pub const MAX_GRANTS_ATTEMPTS: usize = 8;

/// URL schemes a location may use. Each names a store kind; the credential
/// profile names the endpoint or account and the secrets.
pub const SUPPORTED_SCHEMES: [&str; 3] = ["s3", "gs", "az"];

/// One granted location.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Grant {
    pub profile: String,
    pub scheme: String,
    pub bucket: String,
    /// Key prefix inside the bucket, with no leading or trailing `/`. Empty
    /// grants the whole bucket.
    pub prefix: String,
    pub created_unix_ns: i64,
    pub created_by: String,
}

impl Grant {
    /// The grant rendered back as a URL, the form a caller named it by.
    pub fn url(&self) -> String {
        if self.prefix.is_empty() {
            format!("{}://{}", self.scheme, self.bucket)
        } else {
            format!("{}://{}/{}", self.scheme, self.bucket, self.prefix)
        }
    }

    /// The (scheme, bucket, prefix) triple two grants are the same location
    /// by, whatever profile each names.
    fn location(&self) -> (&str, &str, &str) {
        (&self.scheme, &self.bucket, &self.prefix)
    }
}

/// The key part of a parsed location URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPrefix {
    /// The key, with no leading or trailing `/`. Empty means the whole
    /// bucket.
    pub key: String,
    /// True when the URL named a set of objects (it ended in `/`, or named
    /// the whole bucket) rather than one object.
    pub directory: bool,
}

/// A parsed location URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLocation {
    pub scheme: String,
    pub bucket: String,
    pub key: KeyPrefix,
}

/// Why a location URL was refused. Each defect is reported on its own so a
/// caller can say what was wrong with a `LOCATION` rather than only that it
/// did not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationDefect {
    Empty,
    /// No `://`.
    NoScheme,
    /// A scheme outside [`SUPPORTED_SCHEMES`].
    UnsupportedScheme(String),
    /// No bucket between the scheme and the first `/`.
    NoBucket,
    /// A `?` and anything after it: a query selects nothing here, and the
    /// object it would address is not the one the key names.
    Query,
    /// A `#` and anything after it.
    Fragment,
    /// A `%`, which could hide any of the other defects behind an escape.
    PercentEscape,
    /// One of `*`, `?`, `[`, `]`, `{`, `}`: an expanded listing is not the
    /// set of files the statement named.
    Glob,
    /// `//` inside the key: an empty path segment.
    EmptySegment,
    /// A `..` sequence anywhere in the key.
    DotDot,
}

#[derive(Debug, thiserror::Error)]
pub enum GrantsError {
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("grants record {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "grants record {key:?} declares format_version {got}, above the highest version this \
         build reads ({ceiling}): a newer writer produced it. Upgrade this binary rather than \
         rewrite it with fields dropped"
    )]
    UnsupportedVersion { key: String, got: u32, ceiling: u32 },
    #[error(
        "grants record {key:?} declares format_version {got}, below the lowest version this \
         build reads ({floor}): no supported writer produced it"
    )]
    VersionBelowFloor { key: String, got: u32, floor: u32 },
    #[error("invalid location {url:?}: {defect:?}")]
    InvalidLocation { url: String, defect: LocationDefect },
    #[error("a grant needs a credential profile")]
    EmptyProfile,
    #[error(
        "grant {url:?} stores a non-canonical prefix {prefix:?}: a stored prefix carries no \
         leading or trailing slash"
    )]
    NonCanonicalGrant { url: String, prefix: String },
    #[error(
        "location {url:?} overlaps the existing grant {existing:?} of profile \
         {existing_profile:?}, which is not profile {profile:?}: one location would resolve to \
         two credential identities"
    )]
    OverlapsOtherProfile {
        url: String,
        profile: String,
        existing: String,
        existing_profile: String,
    },
    #[error("location {url:?} is already granted to profile {profile:?}")]
    DuplicateGrant { url: String, profile: String },
    #[error("no grant of this tenant admits {url:?}")]
    LocationNotGranted { url: String },
    #[error("no grant of this tenant is exactly {url:?}")]
    GrantNotFound { url: String },
    #[error("the grants record changed under {attempts} consecutive compare-and-swap attempts")]
    RetriesExhausted { attempts: usize },
    #[error(transparent)]
    Key(#[from] KeyError),
}

fn store_error(key: &str, source: StoreError) -> GrantsError {
    GrantsError::Store {
        key: key.to_string(),
        source,
    }
}

/// Parse and canonically check a location URL. Accepts only `s3://`, `gs://`
/// and `az://`, and refuses a URL carrying a query, a fragment, a percent
/// escape, a glob character, an empty path segment or a `..`.
pub fn parse_location(url: &str) -> Result<ParsedLocation, GrantsError> {
    let refuse = |defect| {
        Err(GrantsError::InvalidLocation {
            url: url.to_string(),
            defect,
        })
    };
    if url.is_empty() {
        return refuse(LocationDefect::Empty);
    }
    let Some((scheme, rest)) = url.split_once("://") else {
        return refuse(LocationDefect::NoScheme);
    };
    if !SUPPORTED_SCHEMES.contains(&scheme) {
        return refuse(LocationDefect::UnsupportedScheme(scheme.to_string()));
    }
    if rest.contains('#') {
        return refuse(LocationDefect::Fragment);
    }
    if rest.contains('?') {
        return refuse(LocationDefect::Query);
    }
    if rest.contains('%') {
        return refuse(LocationDefect::PercentEscape);
    }
    if rest.chars().any(|c| GLOB_CHARS.contains(&c)) {
        return refuse(LocationDefect::Glob);
    }
    let (bucket, key) = match rest.split_once('/') {
        Some((bucket, key)) => (bucket, key),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        return refuse(LocationDefect::NoBucket);
    }
    if key.contains("..") {
        return refuse(LocationDefect::DotDot);
    }
    let directory = key.is_empty() || key.ends_with('/');
    let canonical = key.strip_suffix('/').unwrap_or(key);
    if !canonical.is_empty() && canonical.split('/').any(str::is_empty) {
        return refuse(LocationDefect::EmptySegment);
    }
    Ok(ParsedLocation {
        scheme: scheme.to_string(),
        bucket: bucket.to_string(),
        key: KeyPrefix {
            key: canonical.to_string(),
            directory,
        },
    })
}

/// Segment-wise containment of one canonical key prefix in another. An empty
/// `outer` contains everything in its bucket.
fn prefix_contains(outer: &str, inner: &str) -> bool {
    if outer.is_empty() {
        return true;
    }
    inner == outer
        || (inner.len() > outer.len()
            && inner.as_bytes()[outer.len()] == b'/'
            && inner.starts_with(outer))
}

/// True when the two grants name overlapping locations: the same store, and
/// one prefix containing the other.
fn overlaps(a: &Grant, b: &Grant) -> bool {
    a.scheme == b.scheme
        && a.bucket == b.bucket
        && (prefix_contains(&a.prefix, &b.prefix) || prefix_contains(&b.prefix, &a.prefix))
}

/// Does `grant` admit the object (`profile`, `bucket`, `key`)? The read-time
/// check, run against the grants that exist at read time rather than the one
/// the manifest recorded.
///
/// The key is compared as raw bytes against the grant's canonical prefix plus
/// `/`, so `data` admits `data/x.parquet` and never `data2/x.parquet`. A key
/// equal to the prefix is admitted as well, which is what makes a grant of
/// exactly one object usable.
pub fn contains_key(grant: &Grant, profile: &str, bucket: &str, key: &[u8]) -> bool {
    if grant.profile != profile || grant.bucket != bucket {
        return false;
    }
    if grant.prefix.is_empty() {
        return true;
    }
    let prefix = grant.prefix.as_bytes();
    if key == prefix {
        return true;
    }
    key.len() > prefix.len() && key[prefix.len()] == b'/' && key.starts_with(prefix)
}

/// The one grant admitting `url`, and the location's key inside its bucket.
///
/// Grants of one tenant never overlap across profiles ([`add`] refuses that,
/// and [`decode_grants`] refuses a record holding it), so every grant that
/// admits a location names the same profile. Where a tenant nested two grants
/// of its own profile, the most specific one is returned.
pub fn resolve_location(grants: &[Grant], url: &str) -> Result<(Grant, KeyPrefix), GrantsError> {
    let parsed = parse_location(url)?;
    let best = grants
        .iter()
        .filter(|g| {
            g.scheme == parsed.scheme
                && g.bucket == parsed.bucket
                && prefix_contains(&g.prefix, &parsed.key.key)
        })
        .max_by_key(|g| g.prefix.len());
    match best {
        Some(grant) => Ok((grant.clone(), parsed.key)),
        None => Err(GrantsError::LocationNotGranted {
            url: url.to_string(),
        }),
    }
}

/// Check that `grant` may join `grants`: no overlap with another profile, and
/// not a location already granted.
fn check_addable(grants: &[Grant], grant: &Grant) -> Result<(), GrantsError> {
    for existing in grants {
        if existing.profile != grant.profile && overlaps(existing, grant) {
            return Err(GrantsError::OverlapsOtherProfile {
                url: grant.url(),
                profile: grant.profile.clone(),
                existing: existing.url(),
                existing_profile: existing.profile.clone(),
            });
        }
        if existing.location() == grant.location() {
            return Err(GrantsError::DuplicateGrant {
                url: grant.url(),
                profile: existing.profile.clone(),
            });
        }
    }
    Ok(())
}

/// Check a whole record: every grant well formed, no duplicate location, no
/// cross-profile overlap.
pub fn validate_record(grants: &[Grant]) -> Result<(), GrantsError> {
    for (index, grant) in grants.iter().enumerate() {
        if grant.profile.is_empty() {
            return Err(GrantsError::EmptyProfile);
        }
        let url = grant.url();
        let parsed = parse_location(&url)?;
        if parsed.scheme != grant.scheme
            || parsed.bucket != grant.bucket
            || parsed.key.key != grant.prefix
        {
            return Err(GrantsError::NonCanonicalGrant {
                url,
                prefix: grant.prefix.clone(),
            });
        }
        check_addable(&grants[..index], grant)?;
    }
    Ok(())
}

/// Encode a grants record, stamped with [`PARQUET_GRANTS_FORMAT_VERSION`].
/// Refuses a record that fails [`validate_record`].
pub fn encode_grants(grants: &[Grant]) -> Result<Vec<u8>, GrantsError> {
    validate_record(grants)?;
    let body = pb::ParquetGrants {
        format_version: PARQUET_GRANTS_FORMAT_VERSION,
        grants: grants
            .iter()
            .map(|g| pb::ParquetGrant {
                profile: g.profile.clone(),
                scheme: g.scheme.clone(),
                bucket: g.bucket.clone(),
                prefix: g.prefix.clone(),
                created_unix_ns: g.created_unix_ns,
                created_by: g.created_by.clone(),
            })
            .collect(),
    };
    Ok(body.encode_to_vec())
}

/// Decode the grants record stored at `key`. Refuses, with a typed error: a
/// key that is not a grants key, bytes that are not a protobuf message, a
/// `format_version` outside the read window, and a record that fails
/// [`validate_record`].
pub fn decode_grants(key: &str, bytes: &[u8]) -> Result<Vec<Grant>, GrantsError> {
    parse_grants_key(key)?;
    let body = pb::ParquetGrants::decode(bytes).map_err(|source| GrantsError::Decode {
        key: key.to_string(),
        source,
    })?;
    if body.format_version < PARQUET_GRANTS_MIN_READ_VERSION {
        return Err(GrantsError::VersionBelowFloor {
            key: key.to_string(),
            got: body.format_version,
            floor: PARQUET_GRANTS_MIN_READ_VERSION,
        });
    }
    if body.format_version > PARQUET_GRANTS_MAX_READ_VERSION {
        return Err(GrantsError::UnsupportedVersion {
            key: key.to_string(),
            got: body.format_version,
            ceiling: PARQUET_GRANTS_MAX_READ_VERSION,
        });
    }
    let grants: Vec<Grant> = body
        .grants
        .into_iter()
        .map(|g| Grant {
            profile: g.profile,
            scheme: g.scheme,
            bucket: g.bucket,
            prefix: g.prefix,
            created_unix_ns: g.created_unix_ns,
            created_by: g.created_by,
        })
        .collect();
    validate_record(&grants)?;
    Ok(grants)
}

/// Read the record, returning its grants and the CAS token to replace it
/// with. A tenant that has never been granted anything has no record: an
/// empty list and no token.
async fn read(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<(Vec<Grant>, Option<Version>), GrantsError> {
    let key = grants_key(tenant);
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => Ok((decode_grants(&key, &outcome.data)?, Some(outcome.version))),
        Err(StoreError::NotFound) => Ok((Vec::new(), None)),
        Err(source) => Err(store_error(&key, source)),
    }
}

/// Every location granted to `tenant`, in (scheme, bucket, prefix, profile)
/// order.
pub async fn list(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<Vec<Grant>, GrantsError> {
    Ok(read(store, tenant).await?.0)
}

fn put_options(version: &Option<Version>) -> PutOptions {
    match version {
        Some(v) => PutOptions {
            mode: PutMode::CasVersion(v.clone()),
            checksum: None,
        },
        None => PutOptions::create_if_absent(),
    }
}

/// Replace the whole record under CAS, re-reading and re-applying `change` on
/// a lost race. `change` returns the new grant list, or a typed refusal.
async fn replace_whole<T, F>(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    mut change: F,
) -> Result<T, GrantsError>
where
    F: FnMut(Vec<Grant>) -> Result<(Vec<Grant>, T), GrantsError>,
{
    let key = grants_key(tenant);
    for _ in 0..MAX_GRANTS_ATTEMPTS {
        let (grants, version) = read(store, tenant).await?;
        let (mut next, out) = change(grants)?;
        next.sort();
        let bytes = encode_grants(&next)?;
        match store
            .put(&key, Bytes::from(bytes), put_options(&version))
            .await
        {
            Ok(_) => return Ok(out),
            // Another writer replaced the record between the read and the
            // put: re-read and re-decide, since the refusals above are
            // decided against a list that has since moved.
            Err(StoreError::PreconditionFailed | StoreError::AlreadyExists) => continue,
            Err(source) => return Err(store_error(&key, source)),
        }
    }
    Err(GrantsError::RetriesExhausted {
        attempts: MAX_GRANTS_ATTEMPTS,
    })
}

/// Grant `url` to `profile`. Refuses a location that overlaps a grant of
/// another profile in either direction, and a location already granted.
pub async fn add(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    profile: &str,
    url: &str,
    created_by: &str,
    clock: &dyn Clock,
) -> Result<Grant, GrantsError> {
    if profile.is_empty() {
        return Err(GrantsError::EmptyProfile);
    }
    let parsed = parse_location(url)?;
    let grant = Grant {
        profile: profile.to_string(),
        scheme: parsed.scheme,
        bucket: parsed.bucket,
        prefix: parsed.key.key,
        created_unix_ns: clock.now_ns(),
        created_by: created_by.to_string(),
    };
    replace_whole(store, tenant, |mut grants| {
        check_addable(&grants, &grant)?;
        grants.push(grant.clone());
        Ok((grants, grant.clone()))
    })
    .await
}

/// Revoke the grant that is exactly `url`, whatever profile holds it. A
/// location merely admitted by a wider grant is not removed: the caller has
/// to name the grant itself.
pub async fn remove(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    url: &str,
) -> Result<Grant, GrantsError> {
    let parsed = parse_location(url)?;
    let target = (
        parsed.scheme.as_str(),
        parsed.bucket.as_str(),
        parsed.key.key.as_str(),
    );
    replace_whole(store, tenant, |mut grants| {
        let Some(at) = grants.iter().position(|g| g.location() == target) else {
            return Err(GrantsError::GrantNotFound {
                url: url.to_string(),
            });
        };
        let removed = grants.remove(at);
        Ok((grants, removed))
    })
    .await
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::clock::FixedClock;
    use crate::test_util::{TENANT_A, TENANT_B};

    const NOW: i64 = 1_700_000_000_000_000_000;

    fn clock() -> FixedClock {
        FixedClock::new(NOW)
    }

    fn grant(profile: &str, url: &str) -> Grant {
        let parsed = parse_location(url).expect("location");
        Grant {
            profile: profile.into(),
            scheme: parsed.scheme,
            bucket: parsed.bucket,
            prefix: parsed.key.key,
            created_unix_ns: NOW,
            created_by: "operator".into(),
        }
    }

    #[test]
    fn a_location_resolves_to_exactly_one_grant_and_never_by_string_prefix() {
        let data = grant("prod", "s3://b/data");
        let grants = vec![data.clone()];
        // Admitted: the prefix itself, as a directory, and anything a
        // segment below it.
        for url in [
            "s3://b/data",
            "s3://b/data/",
            "s3://b/data/x.parquet",
            "s3://b/data/2026/09/part-0.parquet",
        ] {
            let (got, key) = resolve_location(&grants, url).expect(url);
            assert_eq!(got, data, "{url}");
            assert_eq!(
                key.key,
                url.trim_start_matches("s3://b/").trim_end_matches('/'),
                "{url}"
            );
        }
        assert!(
            resolve_location(&grants, "s3://b/data/")
                .expect("dir")
                .1
                .directory
        );
        assert!(
            !resolve_location(&grants, "s3://b/data/x.parquet")
                .expect("file")
                .1
                .directory
        );
        // Refused: a sibling whose name merely starts with the prefix, a
        // different bucket, a different scheme, and a parent.
        for url in [
            "s3://b/data2/",
            "s3://b/data2/x.parquet",
            "s3://b/database/x.parquet",
            "s3://b/",
            "s3://b/other/x.parquet",
            "s3://other/data/x.parquet",
            "gs://b/data/x.parquet",
            "az://b/data/x.parquet",
        ] {
            assert!(
                matches!(
                    resolve_location(&grants, url),
                    Err(GrantsError::LocationNotGranted { .. })
                ),
                "{url} was admitted"
            );
        }
        // `str::starts_with` containment would admit the first two of those:
        // this is the assertion that fails against it.
        assert!("data2/x.parquet".starts_with("data"));
        assert!(!contains_key(&data, "prod", "b", b"data2/x.parquet"));

        // A whole-bucket grant admits every key in its bucket and nothing
        // outside it, under gs:// and az:// as much as s3://.
        for scheme in SUPPORTED_SCHEMES {
            let whole = grant("prod", &format!("{scheme}://b"));
            assert_eq!(whole.prefix, "");
            let grants = vec![whole.clone()];
            for key in ["", "x.parquet", "any/depth/at/all.parquet"] {
                let url = format!("{scheme}://b/{key}");
                assert_eq!(resolve_location(&grants, &url).expect(&url).0, whole);
                assert!(contains_key(&whole, "prod", "b", key.as_bytes()));
            }
            assert!(matches!(
                resolve_location(&grants, &format!("{scheme}://other/x.parquet")),
                Err(GrantsError::LocationNotGranted { .. })
            ));
        }

        // Every refused URL form is refused before containment is considered.
        let cases: [(&str, LocationDefect); 12] = [
            ("", LocationDefect::Empty),
            ("b/data", LocationDefect::NoScheme),
            (
                "file:///etc/passwd",
                LocationDefect::UnsupportedScheme("file".into()),
            ),
            (
                "S3://b/data",
                LocationDefect::UnsupportedScheme("S3".into()),
            ),
            (
                "https://b/data",
                LocationDefect::UnsupportedScheme("https".into()),
            ),
            ("s3:///data", LocationDefect::NoBucket),
            ("s3://b/data#frag", LocationDefect::Fragment),
            ("s3://b/data?versionId=1", LocationDefect::Query),
            ("s3://b/data%2f..", LocationDefect::PercentEscape),
            ("s3://b/data/*.parquet", LocationDefect::Glob),
            ("s3://b/data//x.parquet", LocationDefect::EmptySegment),
            ("s3://b/data/../secrets", LocationDefect::DotDot),
        ];
        for (url, defect) in cases {
            let got = resolve_location(&grants, url);
            assert!(
                matches!(
                    &got,
                    Err(GrantsError::InvalidLocation { url: u, defect: d })
                        if u == url && *d == defect
                ),
                "{url:?}: {got:?}"
            );
        }
        // A `?` is both a query marker and a glob character; the query
        // defect is the one reported.
        assert!(matches!(
            parse_location("s3://b/d?ta"),
            Err(GrantsError::InvalidLocation {
                defect: LocationDefect::Query,
                ..
            })
        ));
    }

    #[test]
    fn keys_are_compared_as_case_sensitive_bytes() {
        let g = grant("prod", "s3://b/Data");
        assert!(contains_key(&g, "prod", "b", b"Data/x.parquet"));
        assert!(!contains_key(&g, "prod", "b", b"data/x.parquet"));
        // Non-UTF-8 key bytes compare as bytes, not as text.
        assert!(contains_key(
            &g,
            "prod",
            "b",
            &[b'D', b'a', b't', b'a', b'/', 0xff]
        ));
        // Profile and bucket are part of the identity.
        assert!(!contains_key(&g, "staging", "b", b"Data/x.parquet"));
        assert!(!contains_key(&g, "prod", "other", b"Data/x.parquet"));
        // A grant of exactly one object admits that object and nothing beside
        // it.
        let one = grant("prod", "s3://b/data/x.parquet");
        assert!(contains_key(&one, "prod", "b", b"data/x.parquet"));
        assert!(!contains_key(&one, "prod", "b", b"data/x.parquet.bak"));
    }

    #[tokio::test]
    async fn overlapping_grants_are_refused_across_profiles_in_both_directions() {
        let store = MemoryStore::new();
        let clock = clock();
        add(&store, &TENANT_A, "prod", "s3://b/data", "op", &clock)
            .await
            .expect("first grant");
        // The new grant is inside the existing one.
        for url in ["s3://b/data", "s3://b/data/sub", "s3://b/data/sub/deep"] {
            assert!(
                matches!(
                    add(&store, &TENANT_A, "staging", url, "op", &clock).await,
                    Err(GrantsError::OverlapsOtherProfile { .. })
                ),
                "{url} was admitted under a second profile"
            );
        }
        // The new grant contains the existing one, including the whole
        // bucket.
        for url in ["s3://b", "s3://b/"] {
            assert!(
                matches!(
                    add(&store, &TENANT_A, "staging", url, "op", &clock).await,
                    Err(GrantsError::OverlapsOtherProfile { .. })
                ),
                "{url} was admitted under a second profile"
            );
        }
        // Exactly the same location, same profile, is a duplicate.
        assert!(matches!(
            add(&store, &TENANT_A, "prod", "s3://b/data/", "op", &clock).await,
            Err(GrantsError::DuplicateGrant { .. })
        ));
        // A nested grant of the same profile is fine, and the most specific
        // one resolves.
        let nested = add(&store, &TENANT_A, "prod", "s3://b/data/sub", "op", &clock)
            .await
            .expect("nested grant");
        let grants = list(&store, &TENANT_A).await.expect("list");
        assert_eq!(grants.len(), 2);
        assert_eq!(
            resolve_location(&grants, "s3://b/data/sub/x.parquet")
                .expect("resolve")
                .0,
            nested
        );
        assert_eq!(
            resolve_location(&grants, "s3://b/data/other.parquet")
                .expect("resolve")
                .0
                .prefix,
            "data"
        );
        // Neighbouring, non-overlapping locations are fine under any
        // profile, including a different scheme on the same bucket name.
        for (profile, url) in [
            ("staging", "s3://b/data2"),
            ("staging", "s3://other/data"),
            ("staging", "gs://b/data"),
        ] {
            add(&store, &TENANT_A, profile, url, "op", &clock)
                .await
                .unwrap_or_else(|e| panic!("{url}: {e}"));
        }
        // Another tenant's grants are a separate record.
        assert_eq!(list(&store, &TENANT_B).await.expect("list"), vec![]);
        add(&store, &TENANT_B, "staging", "s3://b/data", "op", &clock)
            .await
            .expect("other tenant");
        assert_eq!(list(&store, &TENANT_B).await.expect("list").len(), 1);
    }

    #[tokio::test]
    async fn remove_takes_the_named_grant_and_nothing_wider() {
        let store = MemoryStore::new();
        let clock = clock();
        add(&store, &TENANT_A, "prod", "s3://b/data", "op", &clock)
            .await
            .expect("grant");
        assert!(matches!(
            remove(&store, &TENANT_A, "s3://b/data/x.parquet").await,
            Err(GrantsError::GrantNotFound { .. })
        ));
        assert!(matches!(
            remove(&store, &TENANT_B, "s3://b/data").await,
            Err(GrantsError::GrantNotFound { .. })
        ));
        let removed = remove(&store, &TENANT_A, "s3://b/data/")
            .await
            .expect("remove");
        assert_eq!(removed.prefix, "data");
        assert_eq!(list(&store, &TENANT_A).await.expect("list"), vec![]);
    }

    #[test]
    fn the_grants_read_window_is_exactly_version_one() {
        assert_eq!(PARQUET_GRANTS_FORMAT_VERSION, 1);
        assert_eq!(PARQUET_GRANTS_MIN_READ_VERSION, 1);
        assert_eq!(PARQUET_GRANTS_MAX_READ_VERSION, 1);
    }

    #[test]
    fn a_format_version_outside_the_read_window_is_refused() {
        let key = grants_key(&TENANT_A);
        let grants = vec![grant("prod", "s3://b/data")];
        let bytes = encode_grants(&grants).expect("encode");
        assert_eq!(decode_grants(&key, &bytes).expect("decode"), grants);

        let mut body = pb::ParquetGrants::decode(bytes.as_slice()).expect("decode");
        body.format_version = PARQUET_GRANTS_MAX_READ_VERSION + 1;
        let got = decode_grants(&key, &body.encode_to_vec());
        assert!(
            matches!(
                &got,
                Err(GrantsError::UnsupportedVersion { key: k, got: 2, ceiling: 1 }) if *k == key
            ),
            "{got:?}"
        );
        body.format_version = 0;
        let got = decode_grants(&key, &body.encode_to_vec());
        assert!(
            matches!(
                &got,
                Err(GrantsError::VersionBelowFloor { key: k, got: 0, floor: 1 }) if *k == key
            ),
            "{got:?}"
        );
        // An empty object decodes as an all-default message: version 0.
        assert!(matches!(
            decode_grants(&key, &[]),
            Err(GrantsError::VersionBelowFloor { got: 0, .. })
        ));
        assert!(matches!(
            decode_grants(&key, b"not a protobuf message at all"),
            Err(GrantsError::Decode { .. })
        ));
    }

    #[test]
    fn a_grants_record_read_from_the_wrong_key_is_refused() {
        let bytes = encode_grants(&[grant("prod", "s3://b/data")]).expect("encode");
        for key in [
            format!("t/{}/pq/grantsx", "a1".repeat(16)),
            format!("t/{}/pq/t/hits/v/00000000000000000001.pqm", "a1".repeat(16)),
            format!("t/{}/pq/grants", "a1".repeat(15)),
            "grants".to_string(),
        ] {
            assert!(
                matches!(decode_grants(&key, &bytes), Err(GrantsError::Key(_))),
                "{key:?} decoded"
            );
        }
        // The tenant in the key is the tenant the record belongs to: a valid
        // grants key of another tenant decodes, and the read path never
        // builds one, since it derives the key from its own tenant hash.
        assert!(decode_grants(&grants_key(&TENANT_B), &bytes).is_ok());
    }

    #[test]
    fn a_record_holding_an_overlap_or_a_duplicate_is_refused_on_both_sides() {
        let cross = vec![
            grant("prod", "s3://b/data"),
            grant("staging", "s3://b/data/x"),
        ];
        assert!(matches!(
            encode_grants(&cross),
            Err(GrantsError::OverlapsOtherProfile { .. })
        ));
        let dup = vec![grant("prod", "s3://b/data"), grant("prod", "s3://b/data")];
        assert!(matches!(
            encode_grants(&dup),
            Err(GrantsError::DuplicateGrant { .. })
        ));
        // Built by hand, past the encoder, they are refused on the way back
        // in as well.
        let key = grants_key(&TENANT_A);
        let body = pb::ParquetGrants {
            format_version: PARQUET_GRANTS_FORMAT_VERSION,
            grants: cross
                .iter()
                .map(|g| pb::ParquetGrant {
                    profile: g.profile.clone(),
                    scheme: g.scheme.clone(),
                    bucket: g.bucket.clone(),
                    prefix: g.prefix.clone(),
                    created_unix_ns: g.created_unix_ns,
                    created_by: g.created_by.clone(),
                })
                .collect(),
        };
        assert!(matches!(
            decode_grants(&key, &body.encode_to_vec()),
            Err(GrantsError::OverlapsOtherProfile { .. })
        ));
        // So is a stored prefix that is not canonical.
        let body = pb::ParquetGrants {
            format_version: PARQUET_GRANTS_FORMAT_VERSION,
            grants: vec![pb::ParquetGrant {
                profile: "prod".into(),
                scheme: "s3".into(),
                bucket: "b".into(),
                prefix: "data/".into(),
                created_unix_ns: NOW,
                created_by: "op".into(),
            }],
        };
        assert!(matches!(
            decode_grants(&key, &body.encode_to_vec()),
            Err(GrantsError::NonCanonicalGrant { .. })
        ));
    }

    #[tokio::test]
    async fn two_racing_adds_lose_no_grant() {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        let store = std::sync::Arc::new(store);
        let clock = clock();
        let key = grants_key(&TENANT_A);
        let gate = store.hold(Op::Put, Some(key.clone()), Occurrence::Always);

        let a = tokio::spawn({
            let (store, clock) = (store.clone(), clock.clone());
            async move { add(&*store, &TENANT_A, "prod", "s3://b/one", "op", &clock).await }
        });
        let b = tokio::spawn({
            let (store, clock) = (store.clone(), clock.clone());
            async move { add(&*store, &TENANT_A, "prod", "s3://b/two", "op", &clock).await }
        });

        // Both readers saw no record, so both are about to write a record
        // holding only their own grant.
        gate.wait_until_held(2).await;
        let held = gate.held();
        assert_eq!(held.len(), 2);
        gate.release(held[0]);
        // The second put now fails its CreateIfAbsent and retries against the
        // record the first one wrote.
        gate.release(held[1]);
        gate.wait_until_held(1).await;
        let retry = gate.held();
        assert_eq!(retry.len(), 1);
        gate.release(retry[0]);

        let timeout = std::time::Duration::from_secs(10);
        tokio::time::timeout(timeout, a)
            .await
            .expect("join a")
            .expect("task a")
            .expect("add a");
        tokio::time::timeout(timeout, b)
            .await
            .expect("join b")
            .expect("task b")
            .expect("add b");
        let prefixes: Vec<String> = list(&*store, &TENANT_A)
            .await
            .expect("list")
            .into_iter()
            .map(|g| g.prefix)
            .collect();
        assert_eq!(prefixes, vec!["one".to_string(), "two".to_string()]);
    }
}
