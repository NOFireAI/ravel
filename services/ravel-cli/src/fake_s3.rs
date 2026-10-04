//! A path-style fake S3 endpoint for the store, qualify and tenant-KMS tests
//! (the binary's dispatch tests include this file too): object PUT
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
    /// The `x-amz-server-side-encryption-aws-kms-key-id` header: the KMS key
    /// the PUT asked to be encrypted under, `None` for the bucket default.
    pub sse_kms_key_id: Option<String>,
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
        sse_kms_key_id: headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
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
            // A suffix range, `bytes=-N`: the last N bytes, as a footer-first
            // segment read asks for.
            if start.trim().is_empty() {
                let suffix: usize = end.trim().parse().ok()?;
                let last = len.checked_sub(1)?;
                return (suffix > 0).then_some((len.saturating_sub(suffix), last));
            }
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

/// Tenant data a test seeds through the plain store before it runs a command
/// against the endpoint. The library's tests and the binary's dispatch tests
/// both use it, which is why it sits beside the endpoint they share.
pub(crate) mod seed {
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use ravel_commit::keys;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_types::{Signal, TenantId};
    use uuid::Uuid;

    pub(crate) const NS_PER_HOUR: i64 = 3_600_000_000_000;

    fn logs_record(stream: u8, ts_ns: i64) -> LogRecord {
        let mut id = [0u8; 16];
        id[0] = stream;
        LogRecord {
            stream_id: LogStreamId(id),
            stream_attrs: ravel_logseg::stream_attrs_bytes(
                &[(
                    "service.name".into(),
                    AttrValue::Str(format!("svc-{stream}")),
                )],
                "scope",
                "1",
                &[],
            ),
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "get /api ok".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("code".into(), AttrValue::I64(200))],
        }
    }

    /// Two L0 `.rlog` objects and their commit records in one sealed logs
    /// bucket, enough for a compaction to merge rather than report
    /// `BelowMinInputs`.
    pub(crate) async fn two_l0_logs(
        store: &dyn ObjectStoreBackend,
        tenant: &str,
        shard: u32,
        hour: u32,
    ) {
        let tenant_hash = TenantId::new(tenant).hash();
        let base_ns = i64::from(hour) * NS_PER_HOUR;
        for seq in 1..=2u64 {
            let records: Vec<LogRecord> = (0..4)
                .map(|i| {
                    logs_record(
                        u8::try_from(i % 2).expect("fits u8"),
                        base_ns + i64::from(i) * 1_000_000 + i64::try_from(seq).expect("fits i64"),
                    )
                })
                .collect();
            let writer_id = Uuid::new_v4();
            let identity = ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: seq,
            };
            let mut writer = RlogWriter::new(RlogConfig::default(), identity);
            for r in &records {
                writer.push(r.clone()).expect("push");
            }
            let bytes = Bytes::from(writer.finish().expect("finish L0"));
            let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
            let data_key = keys::data_key(
                &tenant_hash,
                Signal::Logs,
                shard,
                writer_id,
                1,
                seq,
                &content_hash,
            )
            .expect("data key");
            store
                .put(&data_key, bytes.clone(), PutOptions::default())
                .await
                .expect("put data object");

            let streams: BTreeSet<LogStreamId> = records.iter().map(|r| r.stream_id).collect();
            let min_ts = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
            let max_ts = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
            let created = base_ns + i64::try_from(seq).expect("fits i64") * 1_000_000;
            let rec = record::build(NewCommitRecord {
                tenant_hash,
                signal: Signal::Logs,
                shard,
                writer_id,
                writer_epoch: 1,
                writer_seq: seq,
                object_size: bytes.len() as u64,
                content_hash,
                sample_count: records.len() as u64,
                series_count: streams.len() as u64,
                min_event_ts_ns: min_ts,
                max_event_ts_ns: max_ts,
                min_ingest_ts_ns: created,
                max_ingest_ts_ns: created,
                segment_format_version: u32::from(ravel_logseg::footer::VERSION),
                created_unix_ns: created,
                ingest_hour_bucket: hour,
            })
            .expect("build commit record");
            let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
            store
                .put(&commit_key, record::encode(&rec), PutOptions::default())
                .await
                .expect("put commit record");
        }
    }

    /// Publish one sealed metrics commit record and its placeholder data
    /// object.
    pub(crate) async fn metrics_l0(
        store: &dyn ObjectStoreBackend,
        tenant: &str,
        shard: u32,
        seq: u64,
        created_unix_ns: i64,
    ) {
        let tenant_hash = TenantId::new(tenant).hash();
        let ingest_hour_bucket = u32::try_from(created_unix_ns / NS_PER_HOUR).expect("fits u32");
        let payload = format!("seg-{shard}-{seq}").into_bytes();
        let content_hash = *blake3::hash(&payload).as_bytes();
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: seq,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }
}
