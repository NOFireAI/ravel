//! gRPC metadata adapter for the future Flight SQL service (ticket C1c,
//! issue #151).
//!
//! Flight SQL rides on gRPC, so authentication and read-your-write pinning
//! arrive as request metadata rather than as the HTTP headers the REST paths
//! read. This module is the thin translation layer: it turns a
//! [`tonic::metadata::MetadataMap`] into the two values a query needs, reusing
//! the resolution and decode logic the OTLP and SQL paths already established
//! so all three transports reject the same inputs the same way.
//!
//! It deliberately stops there. No [`FlightSqlService`](https://docs.rs/arrow-flight)
//! method lives here, and it does not cross-check the resolved tenant against a
//! ticket's embedded tenant; a later ticket consumes this together with the
//! ticket codec from issue #150.
//!
//! # Tenant resolution
//!
//! [`resolve_tenant`] converts the metadata to a [`HeaderMap`] with the same
//! `metadata_to_headers` helper the OTLP gRPC path uses (reused, not copied),
//! runs the deployment's [`TenantResolver`], and maps a resolution failure to
//! `Status::unauthenticated` with the OTLP path's message. Default-deny is
//! preserved: no resolver, no tenant.
//!
//! # Read-your-write commit tokens
//!
//! Clients pin a query to their own recent writes by attaching one commit
//! token per [`MIN_COMMIT_TOKEN_KEY`] metadata entry. The key is repeated, one
//! token per entry, matching how the SQL endpoint accepts a JSON array of
//! tokens. [`min_commit_tokens`] collects them in the order given and decodes
//! each with [`CommitToken::decode`]; any undecodable value is rejected with
//! `Status::invalid_argument`, mirroring the `build_request` decode-error shape
//! in `sql.rs` (the caller's own input is quoted back so the error is
//! actionable).

use axum::http::HeaderMap;
use ravel_query::http::TenantResolver;
use ravel_types::{CommitToken, TenantHash};
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::otlp_grpc::metadata_to_headers;

/// Metadata key carrying one read-your-write commit token. Repeated: a client
/// attaches the key once per token it wants the query pinned behind, in the
/// order the tokens should be applied.
pub const MIN_COMMIT_TOKEN_KEY: &str = "x-ravel-min-commit-token";

/// Resolves the tenant for a Flight SQL request from its gRPC metadata.
///
/// Reuses `metadata_to_headers` from the OTLP gRPC path so the resolver sees
/// exactly the header shape it does there, then maps a resolution failure to
/// `Status::unauthenticated` with the same message the OTLP path returns.
pub fn resolve_tenant(
    resolver: &dyn TenantResolver,
    metadata: &MetadataMap,
) -> Result<TenantHash, Status> {
    let headers: HeaderMap = metadata_to_headers(metadata);
    resolver
        .resolve(&headers)
        .map(|tenant| tenant.hash())
        .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))
}

/// Collects and decodes the read-your-write commit tokens from the request
/// metadata.
///
/// Reads every [`MIN_COMMIT_TOKEN_KEY`] entry in the order given, decoding each
/// with [`CommitToken::decode`]. Zero entries yield an empty vector. Any
/// undecodable value is rejected with `Status::invalid_argument`, quoting the
/// offending token back to the caller (its own input) as `sql.rs`'s
/// `build_request` does.
pub fn min_commit_tokens(metadata: &MetadataMap) -> Result<Vec<CommitToken>, Status> {
    metadata
        .get_all(MIN_COMMIT_TOKEN_KEY)
        .iter()
        .map(|value| {
            let raw = value
                .to_str()
                .map_err(|_| Status::invalid_argument(format!("invalid {MIN_COMMIT_TOKEN_KEY}")))?;
            CommitToken::decode(raw).map_err(|_| {
                Status::invalid_argument(format!("invalid {MIN_COMMIT_TOKEN_KEY}: {raw:?}"))
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use ravel_query::http::StaticBearerTokenResolver;
    use ravel_types::TenantId;
    use tonic::metadata::MetadataValue;
    use uuid::Uuid;

    /// The deployment's bearer-token resolver, wired with a single known
    /// tenant. `StaticBearerTokenResolver` is the same test double the SQL
    /// endpoint tests use, so there is no bespoke fake to keep in sync.
    fn resolver() -> StaticBearerTokenResolver {
        let mut tokens = HashMap::new();
        tokens.insert("acme-token".to_string(), TenantId::new("acme".to_string()));
        StaticBearerTokenResolver::new(tokens)
    }

    fn bearer_metadata(token: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().expect("ascii value"),
        );
        metadata
    }

    /// A commit token whose encoding is a valid input to `min_commit_tokens`.
    fn sample_token(seq: u64) -> CommitToken {
        CommitToken {
            shard: 3,
            writer_id: Uuid::from_u128(0x1234_5678 + seq as u128),
            epoch: 7,
            seq,
            ingest_hour_bucket: 471_000,
        }
    }

    #[test]
    fn valid_bearer_metadata_resolves_the_expected_tenant() {
        let metadata = bearer_metadata("acme-token");
        let hash = resolve_tenant(&resolver(), &metadata).expect("resolves");
        assert_eq!(hash, TenantId::new("acme".to_string()).hash());
    }

    #[test]
    fn missing_metadata_is_unauthenticated() {
        let metadata = MetadataMap::new();
        let status = resolve_tenant(&resolver(), &metadata).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn an_unknown_bearer_token_is_unauthenticated() {
        let metadata = bearer_metadata("not-a-known-token");
        let status = resolve_tenant(&resolver(), &metadata).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn zero_min_commit_tokens_decode_to_an_empty_vec() {
        let metadata = MetadataMap::new();
        let tokens = min_commit_tokens(&metadata).expect("decodes");
        assert!(tokens.is_empty());
    }

    #[test]
    fn one_min_commit_token_decodes() {
        let expected = sample_token(1);
        let mut metadata = MetadataMap::new();
        metadata.insert(
            MIN_COMMIT_TOKEN_KEY,
            expected.encode().parse().expect("ascii value"),
        );
        let tokens = min_commit_tokens(&metadata).expect("decodes");
        assert_eq!(tokens, vec![expected]);
    }

    #[test]
    fn multiple_min_commit_tokens_decode_in_the_order_given() {
        let first = sample_token(1);
        let second = sample_token(2);
        let third = sample_token(3);

        let mut metadata = MetadataMap::new();
        for token in [&first, &second, &third] {
            let value: MetadataValue<_> = token.encode().parse().expect("ascii value");
            metadata.append(MIN_COMMIT_TOKEN_KEY, value);
        }

        let tokens = min_commit_tokens(&metadata).expect("decodes");
        assert_eq!(tokens, vec![first, second, third]);
    }

    #[test]
    fn an_undecodable_min_commit_token_is_rejected_without_panicking() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            MIN_COMMIT_TOKEN_KEY,
            "not-a-real-token".parse().expect("ascii value"),
        );
        let status = min_commit_tokens(&metadata).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains(MIN_COMMIT_TOKEN_KEY),
            "{}",
            status.message()
        );
    }

    /// A good token followed by a bad one still fails: the whole set is
    /// rejected, never partially applied.
    #[test]
    fn a_valid_token_followed_by_an_invalid_one_is_rejected() {
        let good = sample_token(9);
        let mut metadata = MetadataMap::new();
        metadata.append(
            MIN_COMMIT_TOKEN_KEY,
            good.encode().parse().expect("ascii value"),
        );
        metadata.append(
            MIN_COMMIT_TOKEN_KEY,
            "garbage".parse().expect("ascii value"),
        );
        let status = min_commit_tokens(&metadata).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
}
