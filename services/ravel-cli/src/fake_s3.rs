//! A path-style fake S3 endpoint for the store and qualify tests: object PUT,
//! GET and DELETE over an in-memory map, and the bucket subresource GETs the
//! bucket-protection control plane signs. It records what each PUT carried and
//! which subresources were asked for, so a test can pin both.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

/// Bucket subresources the control plane reads.
const SUBRESOURCES: [&str; 4] = ["versioning", "lifecycle", "replication", "object-lock"];

/// What an unranged GET sent with `x-amz-checksum-mode: ENABLED` returns for
/// an object stored with a checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Echo {
    /// The checksum the PUT carried.
    Stored,
    /// No checksum header.
    Nothing,
    /// A checksum that differs from the one the PUT carried.
    Wrong,
}

/// One object PUT as the endpoint received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeenPut {
    pub key: String,
    /// Every `x-amz-checksum-*` request header, name and value.
    pub checksum_headers: Vec<(String, String)>,
}

pub(crate) struct FakeS3 {
    echo: Echo,
    subresources: HashMap<&'static str, (StatusCode, String)>,
    objects: Mutex<HashMap<String, (Bytes, Option<(String, String)>)>>,
    pub puts: Mutex<Vec<SeenPut>>,
    pub deletes: Mutex<Vec<String>>,
    /// The subresource of every control-plane GET, in arrival order.
    pub control_plane: Mutex<Vec<String>>,
}

impl FakeS3 {
    pub fn puts(&self) -> Vec<SeenPut> {
        self.puts.lock().expect("puts lock").clone()
    }

    pub fn deletes(&self) -> Vec<String> {
        self.deletes.lock().expect("deletes lock").clone()
    }

    pub fn control_plane(&self) -> Vec<String> {
        self.control_plane
            .lock()
            .expect("control plane lock")
            .clone()
    }

    pub fn object_count(&self) -> usize {
        self.objects.lock().expect("objects lock").len()
    }
}

/// Start the endpoint on an ephemeral loopback port. `subresources` answers
/// the control-plane GETs by subresource name; one not listed answers 501, so
/// a test that did not expect the request fails on it.
pub(crate) async fn spawn(
    echo: Echo,
    subresources: &[(&'static str, StatusCode, &str)],
) -> (String, Arc<FakeS3>) {
    let state = Arc::new(FakeS3 {
        echo,
        subresources: subresources
            .iter()
            .map(|(name, status, body)| (*name, (*status, body.to_string())))
            .collect(),
        objects: Mutex::default(),
        puts: Mutex::default(),
        deletes: Mutex::default(),
        control_plane: Mutex::default(),
    });
    let shared = Arc::clone(&state);
    let app = axum::Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = Arc::clone(&shared);
            async move { handle(&state, &method, &uri, &headers, body) }
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (endpoint, state)
}

fn handle(
    state: &FakeS3,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().trim_start_matches('/');
    let key = path.split_once('/').map_or("", |(_bucket, key)| key);
    let query = uri.query().unwrap_or("");
    let subresource = query.split(['=', '&']).next().unwrap_or("");

    if *method == Method::GET && key.is_empty() && SUBRESOURCES.contains(&subresource) {
        state
            .control_plane
            .lock()
            .expect("control plane lock")
            .push(subresource.to_string());
        return match state.subresources.get(subresource) {
            Some((status, body)) => (*status, body.clone()).into_response(),
            None => StatusCode::NOT_IMPLEMENTED.into_response(),
        };
    }

    match *method {
        Method::PUT => put_object(state, key, headers, body),
        Method::GET => get_object(state, key, headers),
        Method::DELETE => {
            delete_object(state, key);
            StatusCode::NO_CONTENT.into_response()
        }
        // `DeleteObjects`, which the S3 client issues for a single delete too.
        Method::POST if key.is_empty() && subresource == "delete" => {
            let text = String::from_utf8_lossy(&body);
            let mut deleted = String::new();
            for part in text.split("<Key>").skip(1) {
                let Some((key, _)) = part.split_once("</Key>") else {
                    continue;
                };
                delete_object(state, key);
                deleted.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
            }
            (
                StatusCode::OK,
                format!("<DeleteResult>{deleted}</DeleteResult>"),
            )
                .into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn delete_object(state: &FakeS3, key: &str) {
    state.objects.lock().expect("objects lock").remove(key);
    state
        .deletes
        .lock()
        .expect("deletes lock")
        .push(key.to_string());
}

fn put_object(state: &FakeS3, key: &str, headers: &HeaderMap, body: Bytes) -> Response {
    let checksum_headers: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| name.as_str().starts_with("x-amz-checksum-"))
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let digest = checksum_headers
        .iter()
        .find(|(name, _)| {
            !matches!(
                name.as_str(),
                "x-amz-checksum-mode" | "x-amz-checksum-algorithm" | "x-amz-checksum-type"
            )
        })
        .cloned();
    state.puts.lock().expect("puts lock").push(SeenPut {
        key: key.to_string(),
        checksum_headers,
    });
    state
        .objects
        .lock()
        .expect("objects lock")
        .insert(key.to_string(), (body, digest));
    (StatusCode::OK, [(header::ETAG, "\"fake-etag\"")], "").into_response()
}

fn get_object(state: &FakeS3, key: &str, headers: &HeaderMap) -> Response {
    let Some((data, digest)) = state
        .objects
        .lock()
        .expect("objects lock")
        .get(key)
        .cloned()
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|spec| spec.strip_prefix("bytes=")?.split_once('-'))
        .map(|(start, end)| {
            let len = data.len();
            let start: usize = start.trim().parse().ok()?;
            let end = match end.trim() {
                "" => len.checked_sub(1)?,
                end => end.parse::<usize>().ok()?.min(len.checked_sub(1)?),
            };
            (start <= end).then_some((start, end))
        });
    let mut response = match range {
        None => (StatusCode::OK, data.clone()).into_response(),
        Some(Some((start, end))) => {
            let mut response =
                (StatusCode::PARTIAL_CONTENT, data.slice(start..end + 1)).into_response();
            if let Ok(value) = format!("bytes {start}-{end}/{}", data.len()).parse() {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            response
        }
        Some(None) => return StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
    };
    let response_headers = response.headers_mut();
    response_headers.insert(header::ETAG, HeaderValue::from_static("\"fake-etag\""));
    response_headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_static("Wed, 21 Oct 2020 07:28:00 GMT"),
    );
    let checksum_mode = headers
        .get("x-amz-checksum-mode")
        .is_some_and(|value| value.as_bytes() == b"ENABLED");
    if range.is_none()
        && checksum_mode
        && let Some((name, value)) = digest
    {
        let returned = match state.echo {
            Echo::Stored => Some(value),
            Echo::Nothing => None,
            Echo::Wrong if value == "AAAAAAAAAAA=" => Some("AQAAAAAAAAA=".to_string()),
            Echo::Wrong => Some("AAAAAAAAAAA=".to_string()),
        };
        if let Some(returned) = returned
            && let (Ok(name), Ok(value)) = (
                axum::http::HeaderName::try_from(name),
                HeaderValue::try_from(returned),
            )
        {
            response_headers.insert(name, value);
        }
    }
    response
}
