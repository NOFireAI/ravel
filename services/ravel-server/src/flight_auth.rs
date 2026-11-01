//! gRPC metadata adapter for the future Flight SQL service.
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
//! ticket's embedded tenant; a later change consumes this together with the
//! ticket codec.
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
//! tokens. Each value is `<token>` or `<table>:<token>`, the forms the SQL
//! endpoint's `min_commit_token` takes (ADR-2904 decision 3). `min_commit_tokens`
//! (built with the `flight-sql` feature) collects them in the order given
//! and parses each with `ravel_sql::MinCommitToken::parse`, the SQL
//! endpoint's parse; any value that does not parse is rejected with
//! `Status::invalid_argument`, quoting the caller's own input and the parse
//! error so the error is actionable.

use axum::http::HeaderMap;
use ravel_query::http::TenantResolver;
use ravel_types::TenantHash;
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

/// Collects and parses the read-your-write commit tokens from the request
/// metadata: every [`MIN_COMMIT_TOKEN_KEY`] entry in the order given, through
/// [`ravel_sql::flight::min_commit_tokens_from_metadata`].
#[cfg(feature = "flight-sql")]
pub fn min_commit_tokens(metadata: &MetadataMap) -> Result<Vec<ravel_sql::MinCommitToken>, Status> {
    ravel_sql::flight::min_commit_tokens_from_metadata(metadata, MIN_COMMIT_TOKEN_KEY)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use ravel_query::http::StaticBearerTokenResolver;
    use ravel_types::TenantId;

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
}

#[cfg(all(test, feature = "flight-sql"))]
#[allow(clippy::expect_used)]
mod token_tests {
    use super::*;

    use ravel_sql::{MinCommitToken, MinCommitTokenParseError};
    use ravel_types::CommitToken;
    use tonic::metadata::MetadataValue;
    use uuid::Uuid;

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

    fn metadata_with(values: &[&str]) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        for value in values {
            let value: MetadataValue<_> = value.parse().expect("ascii value");
            metadata.append(MIN_COMMIT_TOKEN_KEY, value);
        }
        metadata
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
        let tokens = min_commit_tokens(&metadata_with(&[&expected.encode()])).expect("decodes");
        assert_eq!(tokens, vec![MinCommitToken::unqualified(expected)]);
    }

    #[test]
    fn multiple_min_commit_tokens_decode_in_the_order_given() {
        let [first, second, third] = [1, 2, 3].map(sample_token);
        let metadata = metadata_with(&[&first.encode(), &second.encode(), &third.encode()]);
        let tokens = min_commit_tokens(&metadata).expect("decodes");
        assert_eq!(
            tokens,
            vec![
                MinCommitToken::unqualified(first),
                MinCommitToken::unqualified(second),
                MinCommitToken::unqualified(third),
            ]
        );
    }

    /// FLIP: decoding each value with `CommitToken::decode`, as this key did
    /// before ADR-2904 decision 3 reached Flight, refuses `logs:<token>` as
    /// an invalid token.
    #[test]
    fn a_table_qualified_min_commit_token_parses_with_its_table() {
        let logs = sample_token(4);
        let samples = sample_token(5);
        let metadata = metadata_with(&[
            &format!("logs:{}", logs.encode()),
            &format!("samples:{}", samples.encode()),
        ]);
        let tokens = min_commit_tokens(&metadata).expect("a qualified value parses");
        assert_eq!(
            tokens,
            vec![
                MinCommitToken::qualified("logs", logs),
                MinCommitToken::qualified("samples", samples),
            ]
        );
    }

    /// FLIP: a parse of its own instead of `MinCommitToken::parse` (one that
    /// splits at the last colon, or drops an empty qualifier) accepts
    /// `a:b:<token>` or `:<token>`, or refuses them with different text.
    #[test]
    fn a_malformed_qualified_value_is_refused_with_the_parse_error() {
        let encoded = sample_token(6).encode();
        for (raw, err) in [
            (
                format!(":{encoded}"),
                MinCommitTokenParseError::EmptyQualifier,
            ),
            (
                format!("a:b:{encoded}"),
                MinCommitTokenParseError::InvalidToken,
            ),
            (
                "logs:not-a-token".to_owned(),
                MinCommitTokenParseError::InvalidToken,
            ),
        ] {
            let status = min_commit_tokens(&metadata_with(&[&raw])).expect_err("refused");
            assert_eq!(status.code(), tonic::Code::InvalidArgument, "{raw}");
            assert_eq!(
                status.message(),
                format!("invalid {MIN_COMMIT_TOKEN_KEY}: {raw:?}: {err}"),
            );
        }
    }

    #[test]
    fn an_undecodable_min_commit_token_is_rejected_without_panicking() {
        let status =
            min_commit_tokens(&metadata_with(&["not-a-real-token"])).expect_err("rejected");
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
        let good = sample_token(9).encode();
        let status = min_commit_tokens(&metadata_with(&[&good, "garbage"])).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
}
