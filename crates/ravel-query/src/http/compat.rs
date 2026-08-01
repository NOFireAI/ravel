//! Prometheus HTTP API compatibility routes that carry no query semantics:
//! `/api/v1/status/buildinfo` and `/api/v1/metadata`.
//!
//! Grafana's built-in Prometheus datasource probes both on every datasource
//! save and on dashboard load. A 404 there makes the datasource test fail
//! outright, even though every query route works, so these exist purely so
//! that client works against ravel-server.
//!
//! They are stateless, so they live on their own `Router` merged into the
//! query router in [`crate::http::router`] rather than taking [`AppState`].
//!
//! [`AppState`]: crate::http::AppState

use std::collections::BTreeMap;

use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::http::json::ApiResponse;

/// Git revision of the build, when the build environment provided one.
///
/// `option_env!` rather than `env!`: a plain `cargo build` from a source
/// tarball has no git metadata, and Prometheus itself reports an empty
/// `revision` in that case rather than failing to build. Nothing in the
/// repository sets `RAVEL_GIT_SHA` today, so this is the empty string until
/// a build script or CI exports it.
const REVISION: &str = match option_env!("RAVEL_GIT_SHA") {
    Some(sha) => sha,
    None => "",
};

/// `/api/v1/status/buildinfo` `data` object, field-for-field as Prometheus
/// renders it. Fields Ravel has no equivalent for are empty strings, not
/// invented values: `goVersion` in particular must stay empty because Ravel
/// is not Go, and `version` is Ravel's own crate version, never a Prometheus
/// version string. A client that gates features on a Prometheus version
/// should see Ravel's version and treat it as unknown, not be told a
/// Prometheus release Ravel does not implement.
#[derive(Debug, Serialize)]
struct BuildInfo {
    version: &'static str,
    revision: &'static str,
    branch: &'static str,
    #[serde(rename = "buildUser")]
    build_user: &'static str,
    #[serde(rename = "buildDate")]
    build_date: &'static str,
    #[serde(rename = "goVersion")]
    go_version: &'static str,
}

/// Router carrying the stateless Prometheus compatibility routes. Merged into
/// the query router, so any service mounting that router serves these too.
pub fn router() -> Router {
    Router::new()
        .route("/api/v1/status/buildinfo", get(buildinfo))
        .route("/api/v1/metadata", get(metadata))
}

/// Ravel's build info. `version` is this workspace's crate version (every
/// crate in the workspace, `ravel-server` included, inherits the single
/// `[workspace.package] version`), so it is the server's version too.
async fn buildinfo() -> Json<ApiResponse<BuildInfo>> {
    Json(ApiResponse::success(BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        revision: REVISION,
        branch: "",
        build_user: "",
        build_date: "",
        go_version: "",
    }))
}

/// Metric metadata: always an empty object, because Ravel captures no OTLP
/// metric type/help/unit metadata today, and an empty `data` is a valid
/// Prometheus response while inventing types or help text would be a silent
/// lie to the client.
async fn metadata() -> Json<ApiResponse<BTreeMap<String, Vec<()>>>> {
    Json(ApiResponse::success(BTreeMap::new()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn buildinfo_version_is_ravels_own_crate_version() {
        let json = serde_json::to_value(ApiResponse::success(BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            revision: REVISION,
            branch: "",
            build_user: "",
            build_date: "",
            go_version: "",
        }))
        .expect("serializes");
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            json["data"]["goVersion"], "",
            "Ravel is not Go; goVersion stays empty rather than invented"
        );
    }

    #[test]
    fn metadata_data_is_an_empty_object() {
        let json = serde_json::to_value(ApiResponse::success(BTreeMap::<String, Vec<()>>::new()))
            .expect("serializes");
        assert_eq!(json["data"], serde_json::json!({}));
    }
}
