/// Content-addressed cache key (ADR-0046 decision 2): `(tenant_hash,
/// content_hash, offset, len)`.
///
/// Deliberately, there is no constructor that takes an object key string.
/// That is the point of the type: two mutable objects Ravel writes (the
/// catalog HEAD pointer and the maintenance cursor) have no content hash
/// in any `SegmentRef`, so they cannot be named by this key at all. An
/// object-key cache would need an invalidation protocol for those two;
/// this key makes them unrepresentable instead, so there is no protocol
/// to get wrong.
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
}
