//! `ravel-cli tenant parquet-grant` (ADR-2040 decision D1): the external
//! locations an operator admits for one tenant, and the checks a location
//! clears before it is written into the grants record.
//!
//! The durable record and every rule about it belong to
//! [`ravel_pqtable::grants`]: this module parses flags, opens the external
//! store the grant names, runs the two qualification probes, and delegates the
//! record change. There is no CLI-side copy of the location grammar, of the
//! overlap rules, or of the compare-and-swap.
//!
//! # What `add` checks, in order
//!
//! 1. The URL parses and its scheme matches the credential profile's store
//!    kind. `s3://` needs an `s3` profile, `gs://` a `gcs` one, `az://` an
//!    `azure` one. A profile of the wrong kind would reach a different service
//!    with the same bucket name.
//! 2. The granted location holds at least one non-empty object inside it
//!    (the object itself, for a location naming one), preferring a
//!    `.parquet` key but accepting any suffix, found by
//!    [`grants::one_object_under`], and [`probe_preconditions`] qualifies the
//!    store on it. A store that serves a read carrying an ETag it never
//!    issued cannot pin a Parquet file, so a manifest over it would name
//!    bytes that can change underneath a query. A prefix with no such object
//!    is refused too: there is nothing to probe, so the grant would be
//!    admitted unqualified.
//! 3. [`probe_not_ravel_bucket`] qualifies the bucket itself. Anything but a
//!    clean pass is a refusal, including an inconclusive answer: a grant on
//!    Ravel's own bucket under another handle would let an external table read
//!    Ravel's objects across tenants.
//! 4. Only then [`grants::add`] writes the record.
//!
//! # Credential profiles
//!
//! Profiles are read from the JSON file named by the global
//! `--parquet-profiles` flag (or `RAVEL_PARQUET_PROFILES`), through
//! [`load_profiles`], the loader ravel-server uses for the same file, given to
//! it by its own `--parquet-profiles` flag. A profile
//! names its secrets through [`ravel_object_store::external::SecretSource`],
//! whose two forms are an environment variable name and a file path, so the
//! profile file carries a reference to secret material rather than the
//! material.
//!
//! # The test seam
//!
//! [`ExternalStore::open`] always builds a network backend and has no fake
//! kind, so [`add_grant`] takes the opener as a parameter. The clap layer
//! passes [`ExternalStore::open`]; the tests pass a closure returning a second
//! in-memory store, which is what lets the four refusals above be driven end
//! to end.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::external::probe::{probe_not_ravel_bucket, probe_preconditions};
use ravel_object_store::external::{ExternalKind, ExternalProfile, ExternalStore, load_profiles};
use ravel_pqtable::clock::Clock;
use ravel_pqtable::grants::{self, Grant, MAX_PROBE_LIST_PAGES, ProbeObject};
use ravel_types::{TenantHash, TenantId};

/// Opens the store a profile names for one bucket. [`ExternalStore::open`] in
/// the shipping paths; a closure over an in-memory store in tests.
pub type OpenExternal<'a> =
    &'a dyn Fn(&ExternalProfile, &str) -> anyhow::Result<Arc<dyn ObjectStoreBackend>>;

/// A [`Clock`] returning a timestamp the caller already took, so the clock
/// read stays at the clap layer and a test can drive a fixed one.
pub struct AtNs(pub i64);

impl Clock for AtNs {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// Read and validate the credential profile file.
pub fn profiles_from_file(path: &Path) -> anyhow::Result<Vec<ExternalProfile>> {
    let json = std::fs::read_to_string(path)
        .with_context(|| format!("reading the profile file {}", path.display()))?;
    load_profiles(&json)
        .with_context(|| format!("loading external profiles from {}", path.display()))
}

/// The profile named `name`, or an error listing the names that are defined.
pub fn find_profile<'a>(
    profiles: &'a [ExternalProfile],
    name: &str,
) -> anyhow::Result<&'a ExternalProfile> {
    profiles.iter().find(|p| p.name == name).ok_or_else(|| {
        let defined: Vec<&str> = profiles.iter().map(|p| p.name.as_str()).collect();
        anyhow::anyhow!("no credential profile named {name:?}; the file defines {defined:?}")
    })
}

/// Does a location scheme address the store kind this profile configures?
fn kind_admits_scheme(kind: &ExternalKind, scheme: &str) -> bool {
    matches!(
        (kind, scheme),
        (ExternalKind::S3 { .. }, "s3")
            | (ExternalKind::Gcs { .. }, "gs")
            | (ExternalKind::Azure { .. }, "az")
    )
}

/// Grant `url` to `profile` for `tenant`, after the checks this module's
/// header lists. Returns the grant that was written.
///
/// This is the whole of `tenant parquet-grant add` below the clap layer: the
/// external store is reached through `open_external` rather than constructed
/// here, so the same path runs in tests over in-memory stores.
pub async fn add_grant(
    ravel_store: &Arc<dyn ObjectStoreBackend>,
    tenant: &TenantHash,
    profile: &ExternalProfile,
    url: &str,
    created_by: &str,
    clock: &dyn Clock,
    open_external: OpenExternal<'_>,
) -> anyhow::Result<Grant> {
    let parsed = grants::parse_location(url)?;
    if !kind_admits_scheme(&profile.kind, &parsed.scheme) {
        anyhow::bail!(
            "location {url:?} uses the {:?} scheme, which does not address the {:?} store \
             profile {:?} configures: the same bucket name on another service is another \
             bucket",
            parsed.scheme,
            profile.kind.name(),
            profile.name
        );
    }

    let external = open_external(profile, &parsed.bucket)?;
    let candidate = Grant {
        profile: profile.name.clone(),
        scheme: parsed.scheme.clone(),
        bucket: parsed.bucket.clone(),
        prefix: parsed.key.key.clone(),
        created_unix_ns: clock.now_ns(),
        created_by: created_by.to_string(),
    };

    let probe_key = match grants::one_object_under(
        external.as_ref(),
        &candidate,
        &parsed.key.key,
        parsed.key.directory,
    )
    .await
    .with_context(|| format!("looking for an object to probe under {url:?}"))?
    {
        ProbeObject::Found(key) => key,
        ProbeObject::Empty => anyhow::bail!(
            "the location {url:?} holds no object, so the store's preconditions could not be \
             probed on it: grant a location that already holds at least one non-empty \
             object"
        ),
        ProbeObject::PageCapReached => anyhow::bail!(
            "no object under the location {url:?} was found within the first \
             {MAX_PROBE_LIST_PAGES} listing pages, so the store's preconditions could not be \
             probed on it: grant a narrower location, one whose first objects are listed sooner"
        ),
    };
    probe_preconditions(external.as_ref(), &probe_key)
        .await
        .with_context(|| {
            format!(
                "profile {:?} does not qualify for pinned reads of {url:?}, so a manifest over \
                 it could not pin the files it names",
                profile.name
            )
        })?;
    probe_not_ravel_bucket(ravel_store, ravel_store, external.as_ref())
        .await
        .with_context(|| format!("the bucket behind {url:?} did not qualify as external"))?;

    let grant = grants::add(
        ravel_store.as_ref(),
        tenant,
        &profile.name,
        url,
        created_by,
        clock,
    )
    .await?;
    Ok(grant)
}

/// One line per field of one grant, plus the URL those fields spell.
///
/// The destructuring is exhaustive on purpose: a field added to [`Grant`]
/// stops this compiling rather than silently dropping out of `ls`.
fn grant_lines(grant: &Grant) -> Vec<String> {
    let Grant {
        profile,
        scheme,
        bucket,
        prefix,
        created_unix_ns,
        created_by,
    } = grant;
    vec![
        format!("url: {}", grant.url()),
        format!("  profile: {profile}"),
        format!("  scheme: {scheme}"),
        format!("  bucket: {bucket}"),
        format!("  prefix: {prefix}"),
        format!("  created_unix_ns: {created_unix_ns}"),
        format!("  created_by: {created_by}"),
    ]
}

fn print_grant(grant: &Grant) {
    for line in grant_lines(grant) {
        println!("{line}");
    }
}

/// `tenant parquet-grant add`.
pub async fn add(
    store: Arc<dyn ObjectStoreBackend>,
    profiles_path: Option<&Path>,
    tenant: &str,
    location: &str,
    profile_name: &str,
    created_by: &str,
    now_ns: i64,
) -> anyhow::Result<()> {
    let path = require_profiles_path(profiles_path)?;
    let profiles = profiles_from_file(path)?;
    let profile = find_profile(&profiles, profile_name)?;
    let hash = TenantId::new(tenant).hash();
    let grant = add_grant(
        &store,
        &hash,
        profile,
        location,
        created_by,
        &AtNs(now_ns),
        &|profile, bucket| ExternalStore::open(profile, bucket).map_err(anyhow::Error::from),
    )
    .await?;
    println!("granted");
    print_grant(&grant);
    Ok(())
}

/// `tenant parquet-grant remove`.
pub async fn remove(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    location: &str,
) -> anyhow::Result<()> {
    let hash = TenantId::new(tenant).hash();
    let removed = grants::remove(store.as_ref(), &hash, location).await?;
    println!("revoked");
    print_grant(&removed);
    Ok(())
}

/// `tenant parquet-grant ls`.
pub async fn ls(store: Arc<dyn ObjectStoreBackend>, tenant: &str) -> anyhow::Result<()> {
    let hash = TenantId::new(tenant).hash();
    let granted = grants::list(store.as_ref(), &hash).await?;
    if granted.is_empty() {
        println!("no parquet location grants for tenant {tenant}");
        return Ok(());
    }
    println!("{} parquet location grants:", granted.len());
    for grant in &granted {
        print_grant(grant);
    }
    Ok(())
}

fn require_profiles_path(path: Option<&Path>) -> anyhow::Result<&Path> {
    path.ok_or_else(|| {
        anyhow::anyhow!(
            "no credential profile file: pass --parquet-profiles <PATH> (or set \
             RAVEL_PARQUET_PROFILES), the same file ravel-server reads from its own \
             --parquet-profiles flag"
        )
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::external::{GcsProfileCredentials, S3ProfileCredentials};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
        PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
    };

    use super::*;

    const NOW: i64 = 1_700_000_000_000_000_000;

    /// The credential modes here name no secret at all (an instance role,
    /// application default credentials), so no fixture carries key material
    /// and nothing in these tests resolves one: the opener is faked.
    fn s3_profile(name: &str) -> ExternalProfile {
        ExternalProfile {
            name: name.to_string(),
            kind: ExternalKind::S3 {
                region: "us-east-1".into(),
                endpoint: None,
                force_path_style: false,
                allow_http: false,
                credentials: S3ProfileCredentials::InstanceRole,
            },
        }
    }

    fn gcs_profile(name: &str) -> ExternalProfile {
        ExternalProfile {
            name: name.to_string(),
            kind: ExternalKind::Gcs {
                credentials: GcsProfileCredentials::ApplicationDefault,
            },
        }
    }

    /// Ravel's own bucket, as the shared handle [`add_grant`] takes.
    fn ravel_bucket() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// An external bucket holding one object under `data/`.
    async fn external_bucket() -> MemoryStore {
        let store = MemoryStore::new();
        store
            .put(
                "data/part-0.parquet",
                Bytes::from_static(b"parquet bytes"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        store
    }

    /// Hands every `open_external` call the same store, whatever profile and
    /// bucket it names.
    fn opener(
        store: Arc<dyn ObjectStoreBackend>,
    ) -> impl Fn(&ExternalProfile, &str) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
        move |_profile, _bucket| Ok(store.clone())
    }

    /// Forwards everything to the inner store except `get_pinned`, which
    /// serves the object whatever ETag the caller pinned: a store that accepts
    /// an `If-Match` it should refuse.
    struct IgnoresIfMatch<S>(S);

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for IgnoresIfMatch<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.0.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.0.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            _pin: &Pin,
        ) -> Result<PinnedRead, StoreError> {
            self.0.get_with_pin(key, range).await
        }

        async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
            self.0.get_with_pin(key, range).await
        }

        async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
            self.0.pin_of(key).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            self.0.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.0.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.0.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.0.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.0.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.0.capabilities()
        }
    }

    /// Forwards everything to the inner store, but lists the way
    /// `object_store` does behind `S3Store` and `ExternalStore`: a non-empty
    /// prefix is a directory, so `/` is appended to it before listing.
    struct ListsUnderSlash<S>(S);

    impl<S> ListsUnderSlash<S> {
        fn directory(prefix: &str) -> String {
            if prefix.is_empty() {
                String::new()
            } else {
                format!("{}/", prefix.trim_end_matches('/'))
            }
        }
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for ListsUnderSlash<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.0.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.0.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &Pin,
        ) -> Result<PinnedRead, StoreError> {
            self.0.get_pinned(key, range, pin).await
        }

        async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
            self.0.get_with_pin(key, range).await
        }

        async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
            self.0.pin_of(key).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            self.0.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.0.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.0.list(&Self::directory(prefix), page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.0.list_delimited(&Self::directory(prefix)).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.0.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.0.capabilities()
        }
    }

    /// A location naming one object is granted through a store that lists
    /// the way `object_store` does, where listing the object's own key as a
    /// prefix returns nothing.
    #[tokio::test]
    async fn a_grant_of_one_object_finds_it_through_an_object_store_listing() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> =
            Arc::new(ListsUnderSlash(external_bucket().await));
        let listed = external
            .list("data/part-0.parquet", None)
            .await
            .expect("list");
        assert!(listed.objects.is_empty(), "{:?}", listed.objects);

        let grant = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/data/part-0.parquet",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect("grant");
        assert_eq!(grant.prefix, "data/part-0.parquet");
    }

    /// Through the same store a prefix location still lists, with or without
    /// its trailing `/`: `s3://customer/data` names no object, so its HEAD
    /// finds nothing and the prefix is listed.
    #[tokio::test]
    async fn a_prefix_grant_lists_through_an_object_store_listing() {
        for url in ["s3://customer/data/", "s3://customer/data"] {
            let ravel = ravel_bucket();
            let external: Arc<dyn ObjectStoreBackend> =
                Arc::new(ListsUnderSlash(external_bucket().await));
            let grant = add_grant(
                &ravel,
                &TenantId::new("acme").hash(),
                &s3_profile("prod"),
                url,
                "ravel-cli",
                &AtNs(NOW),
                &opener(external),
            )
            .await
            .unwrap_or_else(|err| panic!("{url}: {err:#}"));
            assert_eq!(grant.prefix, "data", "{url}");
        }
    }

    /// The reachability test for issue #2051: `add` drives the profile, both
    /// probes and the grants record end to end, over two in-memory stores, and
    /// the grant it wrote is the one `ls` reads back.
    #[tokio::test]
    async fn add_qualifies_the_store_and_writes_the_grant() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_bucket().await);
        let profile = s3_profile("prod");
        let grant = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &profile,
            "s3://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect("grant");

        assert_eq!(grant.profile, "prod");
        assert_eq!(grant.scheme, "s3");
        assert_eq!(grant.bucket, "customer");
        assert_eq!(grant.prefix, "data");
        assert_eq!(grant.created_unix_ns, NOW);
        assert_eq!(grant.created_by, "ravel-cli");
        assert_eq!(
            grants::list(&ravel, &TenantId::new("acme").hash())
                .await
                .expect("list"),
            vec![grant]
        );
    }

    /// The probe object the bucket check writes to Ravel's own store is
    /// deleted again, so a qualified grant leaves nothing behind under
    /// `sys/pq-probe/`.
    #[tokio::test]
    async fn add_leaves_no_probe_object_in_ravels_bucket() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_bucket().await);
        add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect("grant");
        let left = ravel_object_store::list_all(&ravel, "sys/pq-probe/")
            .await
            .expect("list");
        assert!(left.is_empty(), "{left:?}");
    }

    /// A grant future dropped while the bucket probe's identity read is in
    /// flight still leaves no probe object in Ravel's bucket: the probe's drop
    /// guard deletes it. This is in-process cancellation only. The binary
    /// applies no deadline and installs no signal handler, so a SIGINT ends
    /// the process without running the guard.
    #[tokio::test]
    async fn a_grant_cancelled_mid_probe_leaves_no_probe_object() {
        let ravel = ravel_bucket();
        let external = Arc::new(FaultStore::new(external_bucket().await, FaultPlan::empty()));
        let gate = external.hold(
            Op::Get,
            Some("sys/pq-probe/".to_string()),
            Occurrence::Always,
        );
        let open = opener(external.clone());
        let tenant = TenantId::new("acme").hash();
        let profile = s3_profile("prod");

        let mut grant = Box::pin(add_grant(
            &ravel,
            &tenant,
            &profile,
            "s3://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &open,
        ));
        tokio::select! {
            outcome = &mut grant => panic!("a held identity read cannot complete: {outcome:?}"),
            () = gate.wait_until_held(1) => {}
        }
        let held = gate.held_details();
        assert_eq!(held.len(), 1, "{held:?}");
        let (_, held_op, held_key) = &held[0];
        assert_eq!(*held_op, Op::Get);
        let during = ravel_object_store::list_all(&ravel, "sys/pq-probe/")
            .await
            .expect("list");
        assert_eq!(
            during.iter().map(|m| m.key.clone()).collect::<Vec<_>>(),
            vec![held_key.clone()],
            "the probe object is in Ravel's bucket while its identity read is held"
        );

        drop(grant);
        let mut left = during;
        for _ in 0..100 {
            if left.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
            left = ravel_object_store::list_all(&ravel, "sys/pq-probe/")
                .await
                .expect("list");
        }
        assert!(left.is_empty(), "a cancelled grant left {left:?}");
        let grants = grants::list(&ravel, &tenant).await.expect("list grants");
        assert!(grants.is_empty(), "{grants:?}");
    }

    /// Distinguishing test for the precondition probe: an external store that
    /// serves a read carrying an ETag it never issued is refused, and nothing
    /// is written to the grants record.
    #[tokio::test]
    async fn add_refuses_a_store_that_ignores_if_match() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> =
            Arc::new(IgnoresIfMatch(external_bucket().await));
        let err = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect_err("must refuse");
        let text = format!("{err:#}");
        assert!(text.contains("does not qualify for pinned reads"), "{text}");
        assert!(
            grants::list(&ravel, &TenantId::new("acme").hash())
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// Distinguishing test for the bucket probe: the candidate is Ravel's own
    /// store reached under a second handle, so it serves the probe object
    /// Ravel just wrote and the grant is refused.
    #[tokio::test]
    async fn add_refuses_ravels_own_bucket_under_another_handle() {
        let ravel = ravel_bucket();
        ravel
            .put(
                "data/part-0.parquet",
                Bytes::from_static(b"parquet bytes"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let err = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(ravel.clone()),
        )
        .await
        .expect_err("must refuse");
        let text = format!("{err:#}");
        assert!(text.contains("did not qualify as external"), "{text}");
        assert!(
            grants::list(ravel.as_ref(), &TenantId::new("acme").hash())
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// Distinguishing test for the scheme check: a `gs://` location under an
    /// S3 profile is refused before any store is opened.
    #[tokio::test]
    async fn add_refuses_a_scheme_the_profile_kind_does_not_address() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_bucket().await);
        let err = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "gs://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect_err("must refuse");
        let text = format!("{err:#}");
        assert!(text.contains("does not address the"), "{text}");
        assert!(
            grants::list(&ravel, &TenantId::new("acme").hash())
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// The mirror of the case above: a `gs://` location under a GCS profile
    /// clears the scheme check, so the refusal above is about the kind and not
    /// about the scheme being unusable.
    #[tokio::test]
    async fn a_gs_location_under_a_gcs_profile_is_granted() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_bucket().await);
        let grant = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &gcs_profile("archive"),
            "gs://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect("grant");
        assert_eq!(grant.scheme, "gs");
    }

    /// A grant whose prefix holds no object is refused, and the message says
    /// that is why: there is nothing to run the precondition probe on.
    #[tokio::test]
    async fn add_refuses_a_prefix_that_holds_no_object() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_bucket().await);
        let err = add_grant(
            &ravel,
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/empty/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect_err("must refuse");
        let text = format!("{err:#}");
        assert!(text.contains("holds no object"), "{text}");
    }

    /// A location without a trailing `/` whose HEAD finds a zero-byte object,
    /// as an Azure account with a hierarchical namespace reports a directory,
    /// is listed as a prefix, and the object probed is the file under it.
    #[tokio::test]
    async fn a_zero_byte_directory_blob_is_listed_through() {
        let store = external_bucket().await;
        store
            .put("data", Bytes::new(), PutOptions::default())
            .await
            .expect("put directory blob");
        let external = ListsUnderSlash(store);
        assert_eq!(external.head("data").await.expect("head").size, 0);
        let candidate = Grant {
            profile: "prod".to_string(),
            scheme: "az".to_string(),
            bucket: "customer".to_string(),
            prefix: "data".to_string(),
            created_unix_ns: NOW,
            created_by: "ravel-cli".to_string(),
        };
        assert_eq!(
            grants::one_object_under(&external, &candidate, "data", false)
                .await
                .expect("probe"),
            ProbeObject::Found("data/part-0.parquet".to_string())
        );
    }

    /// A listing that stops at the page bound is not reported as an empty
    /// location: the refusal says no object was found within that many pages
    /// and asks for a narrower location, while a listing that ran to its end
    /// still says the location holds no object.
    #[tokio::test]
    async fn a_listing_stopped_at_the_page_bound_is_refused_as_such() {
        let store = MemoryStore::with_page_size(1);
        // Zero-byte keys are never probeable, so the listing reads every page
        // up to the bound without finding an object.
        for index in 0..=MAX_PROBE_LIST_PAGES {
            store
                .put(
                    &format!("data/{index}.parquet"),
                    Bytes::from_static(b""),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let err = add_grant(
            &ravel_bucket(),
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/data",
            "ravel-cli",
            &AtNs(NOW),
            &opener(Arc::clone(&external)),
        )
        .await
        .expect_err("must refuse");
        let text = format!("{err:#}");
        assert!(
            text.contains(&format!(
                "was found within the first {MAX_PROBE_LIST_PAGES} listing pages"
            )),
            "{text}"
        );
        assert!(text.contains("grant a narrower location"), "{text}");
        assert!(!text.contains("holds no object"), "{text}");

        let err = add_grant(
            &ravel_bucket(),
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/elsewhere/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect_err("must refuse");
        assert!(format!("{err:#}").contains("holds no object"), "{err:#}");
    }

    /// The object probed is one the grant admits: a sibling prefix sharing the
    /// grant's first characters is not offered to the probe, so an empty grant
    /// beside a populated `data2/` is still refused as empty, with or without
    /// the trailing `/` on the location.
    #[tokio::test]
    async fn a_sibling_prefix_does_not_supply_the_probe_object() {
        let store = MemoryStore::new();
        store
            .put(
                "data2/part-0.parquet",
                Bytes::from_static(b"x"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        for url in ["s3://customer/data", "s3://customer/data/"] {
            let err = add_grant(
                &ravel_bucket(),
                &TenantId::new("acme").hash(),
                &s3_profile("prod"),
                url,
                "ravel-cli",
                &AtNs(NOW),
                &opener(Arc::clone(&external)),
            )
            .await
            .expect_err("must refuse");
            assert!(
                format!("{err:#}").contains("holds no object"),
                "{url}: {err:#}"
            );
        }
    }

    /// A zero-byte folder marker, as an S3 console writes one for `data/` and
    /// `data/t1/`, does not refuse a location that holds a real file. The
    /// marker is unaddressable (`Path::from` drops its trailing `/`), so the
    /// listing reports it in `unaddressable` and the probe never sees it: the
    /// probe runs on the file, and the grant is written. A location holding
    /// only the marker is refused as empty.
    #[tokio::test]
    async fn a_leading_folder_marker_does_not_refuse_a_valid_location() {
        let external_store = MemoryStore::new();
        for key in ["data/", "data/t1/"] {
            external_store.insert_foreign(key, Bytes::new());
        }
        external_store
            .put(
                "data/t1/part-0.parquet",
                Bytes::from_static(b"parquet bytes"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let listed = external_store.list("data/", None).await.expect("list");
        let objects: Vec<&str> = listed.objects.iter().map(|m| m.key.as_str()).collect();
        let markers: Vec<&str> = listed
            .unaddressable
            .iter()
            .map(|u| u.key.as_str())
            .collect();
        assert_eq!(objects, ["data/t1/part-0.parquet"]);
        assert_eq!(markers, ["data/", "data/t1/"]);
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_store);
        for url in ["s3://customer/data", "s3://customer/data/t1/"] {
            let ravel = ravel_bucket();
            let tenant = TenantId::new("acme").hash();
            let grant = add_grant(
                &ravel,
                &tenant,
                &s3_profile("prod"),
                url,
                "ravel-cli",
                &AtNs(NOW),
                &opener(Arc::clone(&external)),
            )
            .await
            .unwrap_or_else(|err| panic!("{url}: {err:#}"));
            assert_eq!(
                grants::list(&ravel, &tenant).await.expect("list"),
                vec![grant],
                "{url}"
            );
        }

        let markers_only = MemoryStore::new();
        markers_only.insert_foreign("empty/", Bytes::new());
        let markers_only: Arc<dyn ObjectStoreBackend> = Arc::new(markers_only);
        let err = add_grant(
            &ravel_bucket(),
            &TenantId::new("acme").hash(),
            &s3_profile("prod"),
            "s3://customer/empty/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(markers_only),
        )
        .await
        .expect_err("must refuse");
        assert!(format!("{err:#}").contains("holds no object"), "{err:#}");
    }

    /// Every field of a grant reaches the output `ls` prints. The exhaustive
    /// destructuring in [`grant_lines`] is what makes a new field a compile
    /// error; this pins the values themselves.
    #[test]
    fn every_grant_field_is_printed() {
        let grant = Grant {
            profile: "prod".into(),
            scheme: "s3".into(),
            bucket: "customer".into(),
            prefix: "data".into(),
            created_unix_ns: NOW,
            created_by: "ravel-cli".into(),
        };
        let printed = grant_lines(&grant).join("\n");
        for expected in [
            "url: s3://customer/data",
            "profile: prod",
            "scheme: s3",
            "bucket: customer",
            "prefix: data",
            "created_unix_ns: 1700000000000000000",
            "created_by: ravel-cli",
        ] {
            assert!(
                printed.contains(expected),
                "{expected:?} missing:\n{printed}"
            );
        }
    }

    /// `remove` takes the grant back out, and `ls` then reports none.
    #[tokio::test]
    async fn remove_takes_the_grant_back_out() {
        let ravel = ravel_bucket();
        let external: Arc<dyn ObjectStoreBackend> = Arc::new(external_bucket().await);
        let tenant = TenantId::new("acme").hash();
        add_grant(
            &ravel,
            &tenant,
            &s3_profile("prod"),
            "s3://customer/data/",
            "ravel-cli",
            &AtNs(NOW),
            &opener(external),
        )
        .await
        .expect("grant");
        let removed = grants::remove(&ravel, &tenant, "s3://customer/data/")
            .await
            .expect("remove");
        assert_eq!(removed.prefix, "data");
        assert!(
            grants::list(&ravel, &tenant)
                .await
                .expect("list")
                .is_empty()
        );
    }

    #[test]
    fn a_missing_profile_names_the_ones_that_are_defined() {
        let profiles = vec![s3_profile("prod"), gcs_profile("archive")];
        let err = find_profile(&profiles, "staging").expect_err("must refuse");
        let text = format!("{err:#}");
        assert!(text.contains("\"prod\""), "{text}");
        assert!(text.contains("\"archive\""), "{text}");
    }
}
