//! Cache keys. There are two kinds, and both are `CacheKey`: they differ only
//! in how `content_hash` is derived, so the tiers, the eviction policy and the
//! single-flight map need to know nothing about the difference.
//!
//! - The original kind (ADR-0046 decision 2), built by [`CacheKey::new`]: the
//!   `content_hash` is the object's own BLAKE3, which Ravel computed when it
//!   wrote the object and carries in the `SegmentRef`.
//! - The pinned kind (the ADR-0046 amendment of 2026-09-27, "a pinned key for
//!   external Parquet objects", which applies ADR-2040), built by
//!   [`CacheKey::pinned`]: for a Parquet file Ravel did not write and whose
//!   owner can overwrite it, there is no BLAKE3 of the bytes to key on, so the
//!   `content_hash` is a BLAKE3 over the object's pinned identity
//!   ([`PinnedIdentity`]) instead.
//!
//! The pinned kind is sound only because every read of such an object carries
//! the recorded ETag and version as a precondition
//! (`ravel_object_store::ObjectStoreBackend::get_pinned`), so a cached range
//! under this key is a range of bytes the precondition admits. The amendment
//! states the residual weakness it accepts (a forged MD5 on an unversioned
//! bucket) and why `tenant_hash` bounds it; that argument is not repeated here.

/// Domain separator, so a pinned `content_hash` cannot coincide with the
/// BLAKE3 of any object's bytes: that hash is taken over the bytes
/// themselves, this one over a string that starts with this tag.
const PINNED_DOMAIN: &str = "ravel-cache/pinned-key/v1";

/// The recorded identity of an external object, hashed into the
/// `content_hash` of a pinned [`CacheKey`].
///
/// Every field participates. Two objects that agree on all of them are the
/// same bytes as far as a pinned read can tell, because a read that reaches
/// the cache has already pinned `etag` and `version` on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PinnedIdentity<'a> {
    /// Name of the credential profile the object is read through. Two
    /// profiles can name the same bucket with different access, so it is part
    /// of the identity rather than an attribute of the read.
    pub profile: &'a str,
    /// Bucket or container name.
    pub bucket: &'a str,
    /// Object key, as bytes: object stores admit keys that are not UTF-8.
    pub key: &'a [u8],
    /// The ETag the store reported, verbatim, quotes included.
    pub etag: &'a str,
    /// The version or generation the store reported, when it reports one.
    pub version: Option<&'a str>,
    /// Object size in bytes, as reported by the same HEAD that reported the
    /// ETag.
    pub size: u64,
}

impl PinnedIdentity<'_> {
    /// BLAKE3 over a length-prefixed encoding of every field.
    ///
    /// Length-prefixed rather than delimited or concatenated: with a
    /// separator, a field containing the separator shifts the split, and with
    /// bare concatenation `bucket="ab", key="c"` and `bucket="a", key="bc"`
    /// hash identically. Each field is written as its length in `u64`
    /// little-endian followed by its bytes, so the decoder position after any
    /// field is a function of that field alone. `version` is written as a
    /// presence byte and then, when present, the same length-prefixed form, so
    /// `None` and `Some("")` differ.
    fn content_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        let mut field = |bytes: &[u8]| {
            hasher.update(&(bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        };
        field(PINNED_DOMAIN.as_bytes());
        field(self.profile.as_bytes());
        field(self.bucket.as_bytes());
        field(self.key);
        field(self.etag.as_bytes());
        match self.version {
            Some(version) => {
                field(&[1]);
                field(version.as_bytes());
            }
            None => field(&[0]),
        }
        field(&self.size.to_le_bytes());
        *hasher.finalize().as_bytes()
    }
}

/// Content-addressed cache key (ADR-0046 decision 2): `(tenant_hash,
/// content_hash, offset, len)`.
///
/// Deliberately, there is no constructor that takes an object key string.
/// That is the point of the type: two mutable objects Ravel writes (the
/// catalog HEAD pointer and the maintenance cursor) have no content hash
/// in any `SegmentRef`, so they cannot be named by this key at all. An
/// object-key cache would need an invalidation protocol for those two;
/// this key makes them unrepresentable instead, so there is no protocol
/// to get wrong. [`CacheKey::pinned`] does not reopen that door: it takes
/// an object key only together with an ETag and a size, which the two
/// mutable objects have no recorded value of.
///
/// `tenant_hash` is included even though `content_hash` alone is already
/// unique: it is a defence-in-depth boundary, so a hash collision or a
/// programming error cannot serve one tenant's bytes to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub tenant_hash: [u8; 16],
    pub content_hash: [u8; 32],
    pub offset: u64,
    pub len: u64,
}

impl CacheKey {
    pub fn new(tenant_hash: [u8; 16], content_hash: [u8; 32], offset: u64, len: u64) -> Self {
        CacheKey {
            tenant_hash,
            content_hash,
            offset,
            len,
        }
    }

    /// Key a range of an external object by its pinned identity rather than by
    /// a hash of its bytes (the ADR-0046 pinned-key amendment).
    ///
    /// The resulting key is a `CacheKey` like any other; only the derivation
    /// of `content_hash` differs.
    pub fn pinned(
        tenant_hash: [u8; 16],
        identity: &PinnedIdentity<'_>,
        offset: u64,
        len: u64,
    ) -> Self {
        CacheKey {
            tenant_hash,
            content_hash: identity.content_hash(),
            offset,
            len,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn identity() -> PinnedIdentity<'static> {
        PinnedIdentity {
            profile: "analytics",
            bucket: "customer-lake",
            key: b"year=2026/part-0.parquet",
            etag: "\"d41d8cd98f00b204e9800998ecf8427e\"",
            version: Some("3"),
            size: 4096,
        }
    }

    fn hash_of(identity: &PinnedIdentity<'_>) -> [u8; 32] {
        CacheKey::pinned([7u8; 16], identity, 0, 64).content_hash
    }

    #[test]
    fn a_pinned_key_carries_the_tenant_offset_and_len_it_was_given() {
        let key = CacheKey::pinned([7u8; 16], &identity(), 128, 64);
        assert_eq!(key.tenant_hash, [7u8; 16]);
        assert_eq!(key.offset, 128);
        assert_eq!(key.len, 64);
        assert_eq!(key.content_hash, hash_of(&identity()));
    }

    #[test]
    fn the_same_identity_hashes_the_same_way_twice() {
        assert_eq!(hash_of(&identity()), hash_of(&identity()));
    }

    #[test]
    fn changing_any_single_identity_field_changes_the_content_hash() {
        let base = hash_of(&identity());

        let mut differing = Vec::new();

        let mut profile = identity();
        profile.profile = "analytics2";
        differing.push(("profile", hash_of(&profile)));

        let mut bucket = identity();
        bucket.bucket = "customer-lake-2";
        differing.push(("bucket", hash_of(&bucket)));

        let mut key = identity();
        key.key = b"year=2026/part-1.parquet";
        differing.push(("key", hash_of(&key)));

        let mut etag = identity();
        etag.etag = "\"d41d8cd98f00b204e9800998ecf8427f\"";
        differing.push(("etag", hash_of(&etag)));

        let mut version = identity();
        version.version = Some("4");
        differing.push(("version", hash_of(&version)));

        let mut absent_version = identity();
        absent_version.version = None;
        differing.push(("version absent", hash_of(&absent_version)));

        let mut empty_version = identity();
        empty_version.version = Some("");
        differing.push(("version empty", hash_of(&empty_version)));

        let mut size = identity();
        size.size = 4097;
        differing.push(("size", hash_of(&size)));

        for (field, hash) in &differing {
            assert_ne!(*hash, base, "{field} did not change the content hash");
        }
        for (i, (field, hash)) in differing.iter().enumerate() {
            for (other_field, other) in &differing[i + 1..] {
                assert_ne!(hash, other, "{field} and {other_field} collided");
            }
        }
    }

    #[test]
    fn a_field_boundary_shift_changes_the_content_hash() {
        let left = PinnedIdentity {
            profile: "p",
            bucket: "ab",
            key: b"c",
            etag: "e",
            version: None,
            size: 1,
        };
        let right = PinnedIdentity {
            bucket: "a",
            key: b"bc",
            ..left
        };
        assert_ne!(hash_of(&left), hash_of(&right));
    }

    #[test]
    fn a_pinned_key_differs_from_a_content_addressed_key_over_the_same_bytes() {
        // The domain tag is what separates the two kinds: without it a pinned
        // hash and the BLAKE3 of an object's bytes are drawn from the same
        // space, and nothing stops one object's bytes hashing to another
        // object's identity.
        let identity = identity();
        let mut encoded = Vec::new();
        for field in [
            identity.profile.as_bytes(),
            identity.bucket.as_bytes(),
            identity.key,
            identity.etag.as_bytes(),
        ] {
            encoded.extend_from_slice(&(field.len() as u64).to_le_bytes());
            encoded.extend_from_slice(field);
        }
        let undomained = *blake3::hash(&encoded).as_bytes();
        assert_ne!(hash_of(&identity), undomained);
    }

    #[test]
    fn the_content_addressed_constructor_stores_the_hash_it_is_given() {
        let key = CacheKey::new([1u8; 16], [2u8; 32], 0, 8);
        assert_eq!(key.content_hash, [2u8; 32]);
    }
}
