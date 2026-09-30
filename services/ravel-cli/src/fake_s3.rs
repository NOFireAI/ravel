//! A path-style fake S3 endpoint for the store and qualify tests: object PUT
//! (with `If-None-Match: *` and `If-Match` preconditions), GET, HEAD, DELETE
//! and `ListObjectsV2` over an in-memory map, and the bucket subresource GETs
//! the bucket-protection control plane sends. It honors enough of the object
//! store contract for the conformance suite to pass against it, and records
//! what each PUT carried, each LIST asked for, which keys were read and
//! deleted and which subresources were asked for, so a test can pin them.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

/// Bucket subresources the control plane reads.
const SUBRESOURCES: [&str; 4] = ["versioning", "lifecycle", "replication", "object-lock"];

/// Every object's `Last-Modified`, as a header and as a listing timestamp.
const LAST_MODIFIED_HEADER: &str = "Wed, 21 Oct 2020 07:28:00 GMT";
const LAST_MODIFIED_LISTING: &str = "2020-10-21T07:28:00.000Z";

const ACCESS_DENIED: &str =
    "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";

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

/// One `ListObjectsV2` request as the endpoint received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeenList {
    pub prefix: String,
    pub start_after: Option<String>,
    pub delimiter: Option<String>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    body: Bytes,
    /// The checksum header, name and value, the PUT carried.
    digest: Option<(String, String)>,
    /// Quoted, unique per write.
    etag: String,
}

pub(crate) struct FakeS3 {
    echo: Echo,
    subresources: HashMap<&'static str, (StatusCode, String)>,
    objects: Mutex<HashMap<String, StoredObject>>,
    next_etag: AtomicU64,
    pub puts: Mutex<Vec<SeenPut>>,
    pub lists: Mutex<Vec<SeenList>>,
    /// The key of every object GET the endpoint served.
    pub gets: Mutex<Vec<String>>,
    /// The key of every object HEAD the endpoint served.
    pub heads: Mutex<Vec<String>>,
    pub deletes: Mutex<Vec<String>>,
    /// The subresource of every control-plane GET, in arrival order.
    pub control_plane: Mutex<Vec<String>>,
    /// Answer every delete 403, as for a credential with no delete grant.
    pub refuse_deletes: AtomicBool,
    /// Answer 403 to every PUT whose key ends with this suffix.
    pub refuse_puts_ending: Mutex<Option<String>>,
    /// Answer 403 to every GET or HEAD whose key ends with this suffix.
    pub refuse_gets_ending: Mutex<Option<String>>,
}

impl FakeS3 {
    pub fn puts(&self) -> Vec<SeenPut> {
        self.puts.lock().expect("puts lock").clone()
    }

    pub fn lists(&self) -> Vec<SeenList> {
        self.lists.lock().expect("lists lock").clone()
    }

    pub fn gets(&self) -> Vec<String> {
        self.gets.lock().expect("gets lock").clone()
    }

    pub fn heads(&self) -> Vec<String> {
        self.heads.lock().expect("heads lock").clone()
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

    pub fn has_object(&self, key: &str) -> bool {
        self.objects.lock().expect("objects lock").contains_key(key)
    }

    pub fn refuse_puts_ending(&self, suffix: &str) {
        *self.refuse_puts_ending.lock().expect("refuse lock") = Some(suffix.to_string());
    }

    pub fn refuse_gets_ending(&self, suffix: &str) {
        *self.refuse_gets_ending.lock().expect("refuse lock") = Some(suffix.to_string());
    }

    fn refuses(setting: &Mutex<Option<String>>, key: &str) -> bool {
        setting
            .lock()
            .expect("refuse lock")
            .as_deref()
            .is_some_and(|suffix| key.ends_with(suffix))
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
        next_etag: AtomicU64::new(1),
        puts: Mutex::default(),
        lists: Mutex::default(),
        gets: Mutex::default(),
        heads: Mutex::default(),
        deletes: Mutex::default(),
        control_plane: Mutex::default(),
        refuse_deletes: AtomicBool::new(false),
        refuse_puts_ending: Mutex::default(),
        refuse_gets_ending: Mutex::default(),
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
    let key = percent_decode(path.split_once('/').map_or("", |(_bucket, key)| key), false);
    let query = uri.query().unwrap_or("");
    let subresource = query.split(['=', '&']).next().unwrap_or("");
    let params = query_params(query);

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

    if *method == Method::GET
        && key.is_empty()
        && params.get("list-type").map(String::as_str) == Some("2")
    {
        return list_objects(state, &params);
    }

    let delete = *method == Method::DELETE || (*method == Method::POST && subresource == "delete");
    if delete && state.refuse_deletes.load(Ordering::Relaxed) {
        return (StatusCode::FORBIDDEN, ACCESS_DENIED).into_response();
    }

    match *method {
        Method::PUT if FakeS3::refuses(&state.refuse_puts_ending, &key) => {
            (StatusCode::FORBIDDEN, ACCESS_DENIED).into_response()
        }
        Method::GET | Method::HEAD if FakeS3::refuses(&state.refuse_gets_ending, &key) => {
            (StatusCode::FORBIDDEN, ACCESS_DENIED).into_response()
        }
        Method::PUT => put_object(state, &key, headers, body),
        Method::GET | Method::HEAD => {
            let seen = if *method == Method::HEAD {
                &state.heads
            } else {
                &state.gets
            };
            seen.lock().expect("reads lock").push(key.clone());
            get_object(state, &key, headers)
        }
        Method::DELETE => {
            delete_object(state, &key);
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
    let header_text = |name: header::HeaderName| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let if_none_match = header_text(header::IF_NONE_MATCH);
    let if_match = header_text(header::IF_MATCH);

    let mut objects = state.objects.lock().expect("objects lock");
    let existing = objects.get(key).map(|object| object.etag.clone());
    let precondition_failed = match (&if_none_match, &if_match) {
        (Some(any), _) if any == "*" => existing.is_some(),
        (_, Some(expected)) => existing.as_deref() != Some(expected.as_str()),
        _ => false,
    };
    if precondition_failed {
        return (
            StatusCode::PRECONDITION_FAILED,
            "<Error><Code>PreconditionFailed</Code>\
             <Message>At least one of the pre-conditions you specified did not hold</Message>\
             </Error>",
        )
            .into_response();
    }
    let etag = format!(
        "\"fake-etag-{}\"",
        state.next_etag.fetch_add(1, Ordering::Relaxed)
    );
    objects.insert(
        key.to_string(),
        StoredObject {
            body,
            digest,
            etag: etag.clone(),
        },
    );
    (StatusCode::OK, [(header::ETAG, etag)], "").into_response()
}

fn get_object(state: &FakeS3, key: &str, headers: &HeaderMap) -> Response {
    let Some(object) = state
        .objects
        .lock()
        .expect("objects lock")
        .get(key)
        .cloned()
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let data = object.body;
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
    if let Ok(etag) = HeaderValue::try_from(object.etag) {
        response_headers.insert(header::ETAG, etag);
    }
    response_headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_static(LAST_MODIFIED_HEADER),
    );
    let checksum_mode = headers
        .get("x-amz-checksum-mode")
        .is_some_and(|value| value.as_bytes() == b"ENABLED");
    if range.is_none()
        && checksum_mode
        && let Some((name, value)) = object.digest
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

/// `ListObjectsV2`: keys under `prefix` in key order, strictly after
/// `start-after` or the continuation token, folded into common prefixes under
/// a delimiter, at most `max-keys` entries per response.
fn list_objects(state: &FakeS3, params: &HashMap<String, String>) -> Response {
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let start_after = params.get("start-after").cloned();
    let delimiter = params.get("delimiter").cloned().filter(|d| !d.is_empty());
    state.lists.lock().expect("lists lock").push(SeenList {
        prefix: prefix.clone(),
        start_after: start_after.clone(),
        delimiter: delimiter.clone(),
    });
    let after = params.get("continuation-token").cloned().or(start_after);
    let max_keys = params
        .get("max-keys")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1000)
        .max(1);

    let objects = state.objects.lock().expect("objects lock");
    let mut keys: Vec<&String> = objects
        .keys()
        .filter(|key| key.starts_with(&prefix))
        .filter(|key| {
            after
                .as_ref()
                .is_none_or(|after| key.as_str() > after.as_str())
        })
        .collect();
    keys.sort();

    let mut contents = String::new();
    let mut prefixes = BTreeSet::new();
    let mut entries = 0;
    let mut next = None;
    for key in keys {
        if entries == max_keys {
            next = Some(key.clone());
            break;
        }
        let rest = &key[prefix.len()..];
        if let Some(delimiter) = &delimiter
            && let Some(at) = rest.find(delimiter.as_str())
        {
            let common = &key[..prefix.len() + at + delimiter.len()];
            if prefixes.insert(common.to_string()) {
                entries += 1;
            }
            continue;
        }
        let object = &objects[key];
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{LAST_MODIFIED_LISTING}</LastModified>\
             <ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key),
            xml_escape(&object.etag),
            object.body.len()
        ));
        entries += 1;
    }
    let common: String = prefixes
        .iter()
        .map(|p| {
            format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(p)
            )
        })
        .collect();
    let continuation = next.as_ref().map_or(String::new(), |token| {
        format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(token)
        )
    });
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>ravel-test</Name><Prefix>{}</Prefix><KeyCount>{entries}</KeyCount>\
         <MaxKeys>{max_keys}</MaxKeys><IsTruncated>{}</IsTruncated>{continuation}\
         {contents}{common}</ListBucketResult>",
        xml_escape(&prefix),
        next.is_some()
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}

fn query_params(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(name, true), percent_decode(value, true))
        })
        .collect()
}

/// Decode `%XX` escapes, and `+` as a space when `plus_is_space` (query
/// strings, not paths).
fn percent_decode(text: &str, plus_is_space: bool) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let escaped = (bytes[at] == b'%')
            .then(|| bytes.get(at + 1..at + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match (escaped, bytes[at]) {
            (Some(byte), _) => {
                out.push(byte);
                at += 3;
                continue;
            }
            (None, b'+') if plus_is_space => out.push(b' '),
            (None, byte) => out.push(byte),
        }
        at += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
