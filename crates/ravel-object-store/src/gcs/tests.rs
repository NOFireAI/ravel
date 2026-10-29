use std::sync::Arc;

use bytes::Bytes;
use object_store::client::{HttpConnector, HttpRequestBody};
use object_store::gcp::GcpCredential;
use object_store::{ClientOptions, StaticCredentialProvider};

use super::fake::{FakeGcs, RecordedRequest};
use super::{GcsConfig, GcsStore, is_generation, list_client, store_authority};
use crate::conformance::{Property, run_conformance_suite_with_seeder};
use crate::s3::connector::{self, ObservedStoreTime, S3HttpConnector};
use crate::{
    GetRange, InstrumentedStore, ObjectStoreBackend, PageToken, Pin, PutMode, PutOptions,
    StoreError, StoreMetrics, StoreOp, Version,
};

const BUCKET: &str = "ravel-fake";

async fn fake() -> FakeGcs {
    FakeGcs::start(BUCKET).await.expect("bind the fake")
}

fn store(fake: &FakeGcs) -> GcsStore {
    GcsStore::new(fake.config()).expect("store for the fake")
}

fn cas(version: &Version) -> PutOptions {
    PutOptions {
        mode: PutMode::CasVersion(version.clone()),
        checksum: None,
    }
}

fn is_list_request(request: &RecordedRequest) -> bool {
    request.method == "GET" && request.path == format!("/storage/v1/b/{BUCKET}/o")
}

fn puts_of(fake: &FakeGcs, key: &str) -> Vec<RecordedRequest> {
    fake.requests()
        .into_iter()
        .filter(|r| r.method == "PUT" && r.is_object(BUCKET, key))
        .collect()
}

#[tokio::test]
async fn create_if_absent_sends_generation_zero() {
    let fake = fake().await;
    let store = store(&fake);

    let first = store
        .put(
            "c/k",
            Bytes::from_static(b"first"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("an absent key is created");
    let second = store
        .put(
            "c/k",
            Bytes::from_static(b"second"),
            PutOptions::create_if_absent(),
        )
        .await;

    assert!(
        matches!(second, Err(StoreError::AlreadyExists)),
        "a 412 under CreateIfAbsent is AlreadyExists: {second:?}"
    );
    let puts = puts_of(&fake, "c/k");
    assert_eq!(
        puts.len(),
        2,
        "one request per put, the refused one not retried"
    );
    for put in &puts {
        assert_eq!(put.header("x-goog-if-generation-match"), Some("0"));
    }
    assert_eq!(fake.data_of("c/k").as_deref(), Some(&b"first"[..]));
    assert_eq!(
        first.version.0,
        fake.generation_of("c/k").expect("stored").to_string()
    );
}

#[tokio::test]
async fn cas_sends_the_expected_generation() {
    let fake = fake().await;
    let store = store(&fake);
    let created = store
        .put("cas/k", Bytes::from_static(b"v1"), PutOptions::default())
        .await
        .expect("overwrite");
    fake.clear_requests();

    let updated = store
        .put("cas/k", Bytes::from_static(b"v2"), cas(&created.version))
        .await
        .expect("a CAS on the current generation applies");
    let stale = store
        .put("cas/k", Bytes::from_static(b"v3"), cas(&created.version))
        .await;

    assert!(
        matches!(stale, Err(StoreError::PreconditionFailed)),
        "a 412 under CasVersion is PreconditionFailed: {stale:?}"
    );
    let puts = puts_of(&fake, "cas/k");
    assert_eq!(puts.len(), 2);
    for put in &puts {
        assert_eq!(
            put.header("x-goog-if-generation-match"),
            Some(created.version.0.as_str()),
            "PutMode::Update sends the expected generation as the precondition"
        );
    }
    assert_ne!(updated.version, created.version);
    assert_eq!(fake.data_of("cas/k").as_deref(), Some(&b"v2"[..]));
}

#[tokio::test]
async fn cas_with_a_token_that_is_not_a_generation_sends_nothing() {
    let fake = fake().await;
    let store = store(&fake);
    let created = store
        .put("cas/etag", Bytes::from_static(b"v1"), PutOptions::default())
        .await
        .expect("overwrite");
    fake.clear_requests();

    let refused = store
        .put(
            "cas/etag",
            Bytes::from_static(b"v2"),
            cas(&Version(created.etag.0.clone())),
        )
        .await;

    assert!(
        matches!(refused, Err(StoreError::Permanent(_))),
        "{refused:?}"
    );
    assert!(fake.requests().is_empty(), "{:?}", fake.requests());
}

#[tokio::test]
async fn cas_token_is_the_generation_not_the_etag() {
    let fake = fake().await;
    let store = store(&fake);
    let x = Bytes::from_static(b"identical bytes");

    let first = store
        .put("tok/k", x.clone(), PutOptions::default())
        .await
        .expect("first write");
    let second = store
        .put("tok/k", x.clone(), PutOptions::default())
        .await
        .expect("identical rewrite");

    assert_eq!(
        first.etag, second.etag,
        "an identical rewrite keeps the MD5 ETag"
    );
    assert_ne!(
        first.version, second.version,
        "an identical rewrite moves the token"
    );
    let current = fake.generation_of("tok/k").expect("stored").to_string();
    assert_eq!(second.version.0, current);
    let head = store.head("tok/k").await.expect("head");
    assert_eq!(head.version.0, current);
    let (_, pin) = store.pin_of("tok/k").await.expect("pin_of");
    assert_eq!(pin.version.as_deref(), Some(current.as_str()));
    let got = store.get("tok/k", GetRange::Full).await.expect("get");
    assert_eq!(got.version.0, current);
    let page = store.list("tok/", None).await.expect("list");
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].version.0, current);

    let stale = store
        .put(
            "tok/k",
            Bytes::from_static(b"lost update"),
            cas(&first.version),
        )
        .await;
    assert!(
        matches!(stale, Err(StoreError::PreconditionFailed)),
        "a CAS from the first state cannot overwrite the second: {stale:?}"
    );
    assert_eq!(fake.data_of("tok/k"), Some(x));
}

#[tokio::test]
async fn conformance_suite_passes_against_fake_gcs() {
    let fake = fake().await;
    let mut config = fake.config();
    config.list_page_size = 2;
    let store = GcsStore::new(config).expect("store");
    // A composite object in the bucket for the whole run, beside the suite's
    // root so its exact listings are unchanged.
    let composite = "sys/qualify/composite/blob";
    fake.insert_composite(composite, Bytes::from_static(b"composed"), 2);

    let report =
        run_conformance_suite_with_seeder(&store, "sys/qualify/fake-gcs/", 2, Some(&fake)).await;

    // Every listed ETag, the composite's included, is the one head reports.
    let mut keys = Vec::new();
    let mut page_token = None;
    loop {
        let page = store.list("", page_token).await.expect("list page");
        for listed in &page.objects {
            keys.push(listed.key.clone());
            let head = store.head(&listed.key).await.expect("head");
            assert_eq!(listed.etag, head.etag, "{}", listed.key);
            assert_eq!(listed.version, head.version, "{}", listed.key);
        }
        match page.next {
            Some(next) => page_token = Some(next),
            None => break,
        }
    }
    assert!(keys.iter().any(|key| key == composite), "{keys:?}");

    assert!(report.not_run.is_empty(), "{:?}", report.not_run);
    let failures: Vec<_> = report
        .failures()
        .map(|r| format!("{}: {}", r.property.name(), r.detail))
        .collect();
    assert!(report.passed(), "{failures:#?}");
    let single_winner = report
        .results
        .iter()
        .find(|r| r.property == Property::ConcurrentCreateIfAbsentSingleWinner)
        .expect("the single-winner property is probed");
    assert!(single_winner.passed, "{}", single_winner.detail);
}

#[tokio::test]
async fn attempts_and_observed_store_time_are_recorded() {
    let fake = fake().await;
    let metrics = Arc::new(StoreMetrics::default());
    let store = InstrumentedStore::with_metrics(
        GcsStore::with_metrics(fake.config(), Arc::clone(&metrics)).expect("store"),
        Arc::clone(&metrics),
    );
    assert_eq!(store.observed_store_time_ns(), None);
    fake.set_date_unix_secs(1_800_000_000);

    fake.fail_next(503, 1);
    store
        .put("m/k", Bytes::from_static(b"v"), PutOptions::default())
        .await
        .expect("one 503 is retried");
    assert_eq!(store.observed_store_time_ns(), Some(fake.date_unix_ns()));
    assert_eq!(fake.date_unix_ns(), 1_800_000_000_000_000_000);

    fake.set_date_unix_secs(1_800_000_060);
    store.get("m/k", GetRange::Full).await.expect("get");
    store.head("m/k").await.expect("head");
    store.list("m/", None).await.expect("list");
    store.list_delimited("m/").await.expect("list_delimited");
    store.delete("m/k").await.expect("delete");
    assert_eq!(store.observed_store_time_ns(), Some(fake.date_unix_ns()));

    let snapshot = metrics.snapshot();
    let attempts = |op| snapshot.op(op).attempts;
    let calls = |op| snapshot.op(op).calls;
    assert_eq!(attempts(StoreOp::Put), 2, "the 503 and the retry");
    assert_eq!(calls(StoreOp::Put), 1);
    assert_eq!(attempts(StoreOp::Get), 1);
    assert_eq!(attempts(StoreOp::Head), 1);
    assert_eq!(attempts(StoreOp::List), 1);
    assert_eq!(attempts(StoreOp::ListDelimited), 1);
    assert_eq!(attempts(StoreOp::Delete), 1);
    assert_eq!(fake.requests().len(), 7, "every attempt counted, no more");
}

#[tokio::test]
async fn throttle_and_not_found_classification() {
    let fake = fake().await;
    let store = store(&fake);

    for status in [429, 503] {
        // The initial request and both retries.
        fake.fail_next(status, 3);
        let err = store.get("t/k", GetRange::Full).await;
        assert!(
            matches!(err, Err(StoreError::Throttled { .. })),
            "{status}: {err:?}"
        );
    }
    // A bare 429, with no `SlowDown` code in the body, is throttling too.
    fake.fail_next_without_code(429, 3);
    let err = store.get("t/k", GetRange::Full).await;
    assert!(
        matches!(err, Err(StoreError::Throttled { .. })),
        "bare 429 on get: {err:?}"
    );
    fake.fail_next_without_code(429, 3);
    let err = store.list("t/", None).await;
    assert!(
        matches!(err, Err(StoreError::Throttled { .. })),
        "bare 429 on list: {err:?}"
    );
    for status in [401, 403] {
        fake.fail_next(status, 1);
        let err = store.head("t/k").await;
        assert!(
            matches!(err, Err(StoreError::AccessDenied(_))),
            "{status} on head: {err:?}"
        );
        fake.fail_next(status, 1);
        let err = store
            .put("t/k", Bytes::from_static(b"v"), PutOptions::default())
            .await;
        assert!(
            matches!(err, Err(StoreError::AccessDenied(_))),
            "{status} on put: {err:?}"
        );
    }

    let get = store.get("t/missing", GetRange::Full).await;
    assert!(matches!(get, Err(StoreError::NotFound)), "{get:?}");
    let ranged = store.get("t/missing", GetRange::Suffix(4)).await;
    assert!(matches!(ranged, Err(StoreError::NotFound)), "{ranged:?}");
    let head = store.head("t/missing").await;
    assert!(matches!(head, Err(StoreError::NotFound)), "{head:?}");
    store
        .delete("t/missing")
        .await
        .expect("deleting a missing key succeeds");

    let elsewhere = GcsStore::new(fake.config_for_bucket("no-such-bucket")).expect("store");
    let get = elsewhere.get("t/k", GetRange::Full).await;
    assert!(matches!(get, Err(StoreError::Permanent(_))), "{get:?}");
    let put = elsewhere
        .put("t/k", Bytes::from_static(b"v"), PutOptions::default())
        .await;
    assert!(matches!(put, Err(StoreError::Permanent(_))), "{put:?}");
    let list = elsewhere.list("t/", None).await;
    assert!(matches!(list, Err(StoreError::Permanent(_))), "{list:?}");
    let head = elsewhere.head("t/k").await;
    assert!(matches!(head, Err(StoreError::NotFound)), "{head:?}");
    let delete = elsewhere.delete("t/k").await;
    assert!(
        matches!(delete, Err(StoreError::Permanent(_))),
        "{delete:?}"
    );
}

#[tokio::test]
async fn ranged_reads_select_the_requested_bytes() {
    let fake = fake().await;
    let store = store(&fake);
    store
        .put(
            "r/k",
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put");

    let middle = store
        .get("r/k", GetRange::Range(2, 5))
        .await
        .expect("range");
    assert_eq!(&middle.data[..], b"234");
    assert_eq!(middle.total_size, 10);
    let tail = store.get("r/k", GetRange::Suffix(3)).await.expect("suffix");
    assert_eq!(&tail.data[..], b"789");
    let past = store.get("r/k", GetRange::Range(10, 12)).await;
    assert!(matches!(past, Err(StoreError::InvalidRange(_))), "{past:?}");
    let inverted = store.get("r/k", GetRange::Range(5, 5)).await;
    assert!(matches!(inverted, Err(StoreError::InvalidRange(_))));
    assert_eq!(store.get_unverified(), 0, "ranged reads are not counted");
    store.get("r/k", GetRange::Full).await.expect("full");
    assert_eq!(store.get_unverified(), 1);
}

#[tokio::test]
async fn unverified_reads_are_recorded_on_the_shared_store_metrics() {
    let fake = fake().await;
    let metrics = Arc::new(StoreMetrics::default());
    let store = GcsStore::with_metrics(fake.config(), Arc::clone(&metrics)).expect("store");
    store
        .put("u/k", Bytes::from_static(b"v"), PutOptions::default())
        .await
        .expect("put");

    assert_eq!(metrics.get_unverified(), 0);
    store.get("u/k", GetRange::Full).await.expect("full");
    assert_eq!(
        metrics.get_unverified(),
        1,
        "a full read must count on the shared StoreMetrics, not only on \
         GcsStore's own private counter"
    );
    store.get("u/k", GetRange::Full).await.expect("full");
    assert_eq!(metrics.get_unverified(), 2);
}

#[tokio::test]
async fn pinned_reads_select_the_generation_and_send_the_etag() {
    let fake = fake().await;
    let store = store(&fake);
    store
        .put("p/k", Bytes::from_static(b"one"), PutOptions::default())
        .await
        .expect("put");
    let (_, pin) = store.pin_of("p/k").await.expect("pin_of");
    fake.clear_requests();

    let read = store
        .get_pinned("p/k", GetRange::Full, &pin)
        .await
        .expect("the pinned generation is current");
    assert_eq!(&read.outcome.data[..], b"one");
    assert_eq!(read.pin, pin);
    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].query_param("generation"),
        pin.version.as_deref()
    );
    assert_eq!(requests[0].header("if-match"), Some(pin.etag.as_str()));

    // Same bytes, same ETag: only the generation tells the two apart.
    store
        .put("p/k", Bytes::from_static(b"one"), PutOptions::default())
        .await
        .expect("identical rewrite");
    let gone = store.get_pinned("p/k", GetRange::Full, &pin).await;
    assert!(matches!(gone, Err(StoreError::NotFound)), "{gone:?}");
}

#[tokio::test]
async fn list_and_head_agree_on_the_etag_of_single_and_composite_objects() {
    let fake = fake().await;
    let store = store(&fake);
    fake.insert_object("e/single", Bytes::from_static(b"one upload"));
    fake.insert_composite("e/composite", Bytes::from_static(b"two parts"), 2);
    let composite_etag = fake.etag_of("e/composite").expect("planted");
    assert!(composite_etag.starts_with("\"-"), "{composite_etag}");

    let page = store.list("e/", None).await.expect("list");
    let delimited = store.list_delimited("e/").await.expect("delimited");

    let listed: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(listed, ["e/composite", "e/single"]);
    for (from_list, from_delimited) in page.objects.iter().zip(&delimited.objects) {
        let (head, pin) = store.pin_of(&from_list.key).await.expect("pin_of");
        assert_eq!(from_list.etag, head.etag, "{}", from_list.key);
        assert_eq!(from_delimited.etag, head.etag, "{}", from_list.key);
        assert_eq!(Some(from_list.etag.0.clone()), fake.etag_of(&from_list.key));
        // The listed ETag and generation are a pin that reads the object.
        let listed_pin =
            Pin::from_store(from_list.etag.0.clone(), Some(from_list.version.0.clone()));
        assert_eq!(listed_pin, pin);
        store
            .get_pinned(&from_list.key, GetRange::Full, &listed_pin)
            .await
            .expect("the listed pin reads the object");
    }
    assert_eq!(page.objects[0].etag.0, composite_etag);
}

#[tokio::test]
async fn listing_asks_for_only_the_fields_it_reads_and_the_fake_cuts_to_them() {
    let fake = fake().await;
    fake.insert_object("f/k", Bytes::from_static(b"v"));
    let store = store(&fake);
    store.list("f/", None).await.expect("list");
    let lists: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(is_list_request)
        .collect();
    assert_eq!(lists.len(), 1);
    assert_eq!(
        lists[0].query_param("fields"),
        Some(
            "items(name,generation,size,updated,etag,md5Hash,componentCount),prefixes,\
             nextPageToken"
        )
    );

    let client = reqwest::Client::new();
    let url = format!("{}/storage/v1/b/{BUCKET}/o", fake.endpoint());
    let cut = client
        .get(format!(
            "{url}?fields=items%28name%2Csize%29%2CnextPageToken"
        ))
        .send()
        .await
        .expect("send")
        .text()
        .await
        .expect("body");
    let cut: serde_json::Value = serde_json::from_str(&cut).expect("json");
    assert_eq!(
        cut,
        serde_json::json!({ "items": [{ "name": "f/k", "size": "1" }] })
    );
    let refused = client
        .get(format!("{url}?fields=items%28name"))
        .send()
        .await
        .expect("send");
    assert_eq!(refused.status(), reqwest::StatusCode::BAD_REQUEST);
}

/// The production path: a listing client holding a credential provider sends
/// its bearer token on every request.
#[tokio::test]
async fn the_listing_client_sends_the_credential_providers_bearer_token() {
    let fake = fake().await;
    fake.insert_object("auth/k", Bytes::from_static(b"v"));
    let mut config = fake.config();
    config.skip_signature = false;
    let credentials = Arc::new(StaticCredentialProvider::new(GcpCredential {
        bearer: "listing-bearer".to_string(),
    }));
    let lister = list_client(
        &config,
        Arc::default(),
        Arc::default(),
        store_authority(config.base_url.as_deref()).expect("authority"),
        Some(credentials),
    )
    .expect("list client");

    let page = lister.list_page("auth/", None).await.expect("list");
    lister
        .list_delimited("auth/", crate::MAX_LIST_PAGES)
        .await
        .expect("delimited");

    assert_eq!(page.objects.len(), 1);
    let lists: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(is_list_request)
        .collect();
    assert_eq!(lists.len(), 2);
    for list in &lists {
        assert_eq!(list.header("authorization"), Some("Bearer listing-bearer"));
    }
}

#[tokio::test]
async fn listing_pages_carry_generations_and_report_unaddressable_keys() {
    let fake = fake().await;
    let mut config = fake.config();
    config.list_page_size = 2;
    let store = GcsStore::new(config).expect("store");
    for key in ["l/a", "l/b", "l/c", "l/d/e"] {
        store
            .put(key, Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("put");
    }
    fake.insert_object("l//foreign", Bytes::from_static(b"x"));

    let mut keys = Vec::new();
    let mut unaddressable = Vec::new();
    let mut page_token = None;
    loop {
        let page = store.list("l/", page_token).await.expect("list page");
        for object in &page.objects {
            assert_eq!(
                object.version.0,
                fake.generation_of(&object.key).expect("listed").to_string()
            );
            assert_eq!(Some(object.etag.0.clone()), fake.etag_of(&object.key));
        }
        keys.extend(page.objects.into_iter().map(|o| o.key));
        unaddressable.extend(page.unaddressable.into_iter().map(|u| u.key));
        match page.next {
            Some(next) => page_token = Some(next),
            None => break,
        }
    }
    assert_eq!(keys, ["l/a", "l/b", "l/c", "l/d/e"]);
    assert_eq!(unaddressable, ["l//foreign"]);
    let lists = fake
        .requests()
        .into_iter()
        .filter(is_list_request)
        .collect::<Vec<_>>();
    assert!(!lists.is_empty());
    assert!(
        lists
            .iter()
            .all(|r| r.query_param("maxResults") == Some("2")),
        "{lists:?}"
    );

    let after = store
        .list_after("l/", Some("l/b"), None)
        .await
        .expect("after");
    assert_eq!(after.objects.first().map(|o| o.key.as_str()), Some("l/c"));

    let delimited = store.list_delimited("l/").await.expect("delimited");
    let direct: Vec<_> = delimited.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(direct, ["l/a", "l/b", "l/c"]);
    assert_eq!(delimited.common_prefixes, ["l/d/"]);
}

#[tokio::test]
async fn multipart_is_refused_and_capabilities_say_so() {
    let fake = fake().await;
    let store = store(&fake);
    let caps = store.capabilities();
    assert!(!caps.multipart);
    assert!(!caps.upload_checksum);
    assert!(caps.consistent_read && caps.consistent_list);
    assert!(caps.create_if_absent && caps.cas_version);
    assert!(caps.suffix_range && caps.prefix_list);
    assert!(store.put_multipart("mp/k").await.is_err());
    assert!(fake.requests().is_empty(), "{:?}", fake.requests());
}

#[test]
fn config_refuses_test_only_settings_without_a_base_url() {
    let mut http = GcsConfig::new("b");
    http.allow_http = true;
    let mut unsigned = GcsConfig::new("b");
    unsigned.skip_signature = true;
    let mut no_page = GcsConfig::new("b");
    no_page.list_page_size = 0;
    for config in [GcsConfig::new(""), http, unsigned, no_page] {
        assert!(
            matches!(config.validate(), Err(StoreError::Permanent(_))),
            "{config:?}"
        );
    }
    let defaults = GcsConfig::new("b");
    assert!(defaults.validate().is_ok());
    assert_eq!(defaults.request_timeout, std::time::Duration::from_secs(20));
    assert_eq!(defaults.connect_timeout, std::time::Duration::from_secs(3));
}

#[test]
fn the_store_authority_is_the_base_urls_host_and_port() {
    let authority = |url| store_authority(url).expect("an authority");
    assert_eq!(authority(None), "storage.googleapis.com");
    assert_eq!(authority(Some("http://127.0.0.1:4443")), "127.0.0.1:4443");
    assert_eq!(
        authority(Some("https://gcs.example:8443/base?x@y#z")),
        "gcs.example:8443"
    );
    assert!(matches!(
        store_authority(Some("http:///path")),
        Err(StoreError::Permanent(_))
    ));
}

#[test]
fn a_base_url_with_userinfo_is_refused_without_echoing_it() {
    for url in [
        "https://user:hunter2@gcs.example:8443/base",
        "https://hunter2@gcs.example",
    ] {
        match store_authority(Some(url)) {
            Err(StoreError::Permanent(message)) => {
                assert!(message.contains("userinfo"), "{message}");
                assert!(!message.contains("hunter2"), "{message}");
            }
            other => panic!("{url}: {other:?}"),
        }
        let mut config = GcsConfig::new("b");
        config.base_url = Some(url.to_string());
        match GcsStore::new(config) {
            Err(StoreError::Permanent(message)) => assert!(!message.contains("hunter2")),
            other => panic!("{url}: {other:?}"),
        }
    }
}

#[test]
fn generations_are_positive_int64_decimals() {
    for token in ["1", "1790000000000001", "9223372036854775807"] {
        assert!(is_generation(token), "{token:?}");
    }
    for token in [
        "",
        "0",
        "00",
        "012",
        "abc",
        "+1",
        "-1",
        " 1",
        "1 ",
        "9223372036854775808",
    ] {
        assert!(!is_generation(token), "{token:?}");
    }
}

/// The GCS client sends its credential requests through the same connector;
/// a request to any host but the store's is neither an attempt nor a reading
/// of the store's clock.
#[tokio::test]
async fn requests_to_another_host_are_not_store_attempts() {
    let store_fake = fake().await;
    let token_endpoint = FakeGcs::start("token").await.expect("bind");
    token_endpoint.set_date_unix_secs(1_000_000_000);
    let metrics = Arc::new(StoreMetrics::default());
    let store_time = Arc::new(ObservedStoreTime::default());
    let authority = store_authority(Some(store_fake.endpoint())).expect("authority");
    let client = S3HttpConnector::new(Arc::clone(&metrics), Arc::clone(&store_time))
        .only_for_authority(authority)
        .connect(&ClientOptions::new().with_allow_http(true))
        .expect("client");
    let request = |url: String| {
        axum::http::Request::builder()
            .uri(url)
            .body(HttpRequestBody::from(Bytes::new()))
            .expect("request")
    };

    connector::scope(StoreOp::Get, async {
        let elsewhere = client
            .execute(request(format!("{}/token/k", token_endpoint.endpoint())))
            .await;
        assert!(elsewhere.is_ok(), "{elsewhere:?}");
        assert_eq!(metrics.snapshot().op(StoreOp::Get).attempts, 0);
        assert_eq!(store_time.latest(), None);

        let to_store = client
            .execute(request(format!("{}/{BUCKET}/k", store_fake.endpoint())))
            .await;
        assert!(to_store.is_ok(), "{to_store:?}");
    })
    .await;

    assert_eq!(token_endpoint.requests().len(), 1);
    assert_eq!(store_fake.requests().len(), 1);
    assert_eq!(metrics.snapshot().op(StoreOp::Get).attempts, 1);
    assert_eq!(store_time.latest(), Some(store_fake.date_unix_ns()));
}

fn paged_store(fake: &FakeGcs, page_size: usize) -> GcsStore {
    let mut config = fake.config();
    config.list_page_size = page_size;
    GcsStore::new(config).expect("store")
}

#[tokio::test]
async fn listing_reports_xml_illegal_and_unaddressable_names_without_failing_the_page() {
    let fake = fake().await;
    let store = paged_store(&fake, 2);
    let xml_illegal = "x/b\u{1}";
    let unaddressable_name = "x/c//d";
    assert!(!crate::is_addressable_key(xml_illegal));
    assert!(!crate::is_addressable_key(unaddressable_name));
    for key in ["x/a", xml_illegal, "x/c", unaddressable_name, "x/e", "x/f"] {
        fake.insert_object(key, Bytes::from_static(b"v"));
    }

    let (keys, unaddressable, per_call) = list_counting_requests(&store, &fake, "x/").await;

    assert_eq!(keys, ["x/a", "x/c", "x/e", "x/f"]);
    assert_eq!(unaddressable, [xml_illegal, unaddressable_name]);
    // Each resumed response holds the start name and one more, which ends the
    // call: five calls of one request each, never a second request per call.
    assert_eq!(per_call, [1, 1, 1, 1, 1]);
    let requests = fake.requests();
    assert!(
        requests
            .iter()
            .all(|r| r.query_param("list-type").is_none()),
        "no S3-style listing: {requests:?}"
    );
}

/// Follows `list` to the end, returning the keys and the unaddressable names
/// in the order the pages reported them, and the `objects.list` requests each
/// call sent.
async fn list_counting_requests(
    store: &GcsStore,
    fake: &FakeGcs,
    prefix: &str,
) -> (Vec<String>, Vec<String>, Vec<usize>) {
    let mut keys = Vec::new();
    let mut unaddressable = Vec::new();
    let mut per_call = Vec::new();
    let mut page_token = None;
    let mut sent = 0;
    loop {
        let page = store.list(prefix, page_token).await.expect("list page");
        let total = fake.requests().into_iter().filter(is_list_request).count();
        per_call.push(total - sent);
        sent = total;
        keys.extend(page.objects.into_iter().map(|o| o.key));
        unaddressable.extend(page.unaddressable.into_iter().map(|u| u.key));
        match page.next {
            Some(next) => page_token = Some(next),
            None => break,
        }
    }
    (keys, unaddressable, per_call)
}

/// Ten names at three a page: the first call returns three names, each later
/// one two (its response's other name is the inclusive start name), so the
/// drain is five calls of one request each, five requests in all.
#[tokio::test]
async fn a_resumed_call_returns_a_short_page_rather_than_a_second_request() {
    let fake = fake().await;
    let store = paged_store(&fake, 3);
    let names: Vec<String> = ('a'..='j').map(|c| format!("n/{c}")).collect();
    for name in &names {
        fake.insert_object(name, Bytes::from_static(b"v"));
    }

    let mut pages = Vec::new();
    let mut per_call = Vec::new();
    let mut page_token = None;
    loop {
        fake.clear_requests();
        let page = store.list("n/", page_token).await.expect("list page");
        per_call.push(fake.requests().into_iter().filter(is_list_request).count());
        pages.push(
            page.objects
                .iter()
                .map(|o| o.key.trim_start_matches("n/").to_string())
                .collect::<String>(),
        );
        match page.next {
            Some(next) => page_token = Some(next),
            None => break,
        }
    }

    assert_eq!(pages, ["abc", "de", "fg", "hi", "j"]);
    assert_eq!(per_call, [1, 1, 1, 1, 1]);
}

#[tokio::test]
async fn list_after_skips_the_start_key() {
    let fake = fake().await;
    let store = paged_store(&fake, 2);
    for key in ["s/a", "s/b", "s/c", "s/d"] {
        fake.insert_object(key, Bytes::from_static(b"v"));
    }

    let page = store
        .list_after("s/", Some("s/b"), None)
        .await
        .expect("list_after");

    // The one response holds `s/b` (startOffset is inclusive) and `s/c`; the
    // call returns `s/c` alone rather than sending a second request.
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["s/c"]);
    assert_eq!(page.next, Some(PageToken("s/c".to_string())));
    let lists: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(is_list_request)
        .collect();
    assert_eq!(lists.len(), 1, "{lists:?}");
    assert_eq!(lists[0].query_param("startOffset"), Some("s/b"));
    assert_eq!(lists[0].query_param("pageToken"), None);

    let rest = store
        .list_after("s/", Some("s/b"), page.next)
        .await
        .expect("list_after");
    let keys: Vec<_> = rest.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["s/d"]);
    assert_eq!(rest.next, None);
}

#[tokio::test]
async fn listing_sends_the_page_size_and_resumes_from_the_page_token() {
    let fake = fake().await;
    let store = paged_store(&fake, 3);
    // Three entries a page: the first ends at `d/c`, the second holds `d/d`.
    for key in ["d/a", "d/b", "d/c", "d/d"] {
        fake.insert_object(key, Bytes::from_static(b"v"));
    }

    let page = store.list("d/", None).await.expect("list");
    let direct: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(direct, ["d/a", "d/b", "d/c"]);
    let rest = store
        .list_after("d/", Some("d/c"), None)
        .await
        .expect("list_after");
    let after: Vec<_> = rest.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(after, ["d/d"]);

    let lists: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(is_list_request)
        .collect();
    assert_eq!(lists.len(), 2, "{lists:?}");
    for list in &lists {
        assert_eq!(list.query_param("maxResults"), Some("3"));
        assert_eq!(list.query_param("prefix"), Some("d/"));
        assert_eq!(
            list.header("authorization"),
            None,
            "skip_signature lists unsigned"
        );
    }
}

/// `list_delimited` drains every response regardless of the store's
/// configured page size, so it asks for the wire maximum rather than the
/// configured one (ADR-3456 decision 1).
#[tokio::test]
async fn delimited_listing_sends_the_wire_maximum_not_the_page_size() {
    let fake = fake().await;
    let store = paged_store(&fake, 2);
    for key in ["d/a", "d/b", "d/sub/x"] {
        fake.insert_object(key, Bytes::from_static(b"v"));
    }

    let delimited = store.list_delimited("d/").await.expect("delimited");
    let direct: Vec<_> = delimited.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(direct, ["d/a", "d/b"]);
    assert_eq!(delimited.common_prefixes, ["d/sub/"]);

    let lists: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(is_list_request)
        .collect();
    assert_eq!(lists.len(), 1, "{lists:?}");
    assert_eq!(lists[0].query_param("maxResults"), Some("1000"));
    assert_eq!(lists[0].query_param("delimiter"), Some("/"));
}

#[tokio::test]
async fn cas_rejects_a_non_generation_token_before_any_request() {
    let fake = fake().await;
    let store = store(&fake);
    store
        .put("cas/t", Bytes::from_static(b"v1"), PutOptions::default())
        .await
        .expect("overwrite");
    fake.clear_requests();

    for token in [
        "0",
        "00",
        "012",
        "",
        "abc",
        "+1",
        "-1",
        " 1",
        "9223372036854775808",
    ] {
        let refused = store
            .put(
                "cas/t",
                Bytes::from_static(b"v2"),
                cas(&Version(token.to_string())),
            )
            .await;
        assert!(
            fake.requests().is_empty(),
            "{token:?}: {:?}",
            fake.requests()
        );
        assert!(
            matches!(refused, Err(StoreError::Permanent(_))),
            "{token:?}: {refused:?}"
        );
    }
    assert_eq!(fake.data_of("cas/t").as_deref(), Some(&b"v1"[..]));
}

#[tokio::test]
async fn pinned_read_ordering() {
    let fake = fake().await;
    let store = store(&fake);
    let bytes = Bytes::from_static(b"pinned");

    let absent = Pin::from_store("\"0123456789abcdef0123456789abcdef\"", Some("1".into()));
    let read = store.get_pinned("o/absent", GetRange::Full, &absent).await;
    assert!(
        matches!(read, Err(StoreError::NotFound)),
        "absent key with a pin: {read:?}"
    );

    store
        .put("o/k", bytes.clone(), PutOptions::default())
        .await
        .expect("put");
    let (_, live) = store.pin_of("o/k").await.expect("pin_of");
    let wrong_etag = Pin::from_store("\"0123456789abcdef0123456789abcdef\"", live.version.clone());
    let read = store.get_pinned("o/k", GetRange::Full, &wrong_etag).await;
    assert!(
        matches!(read, Err(StoreError::PreconditionFailed)),
        "live generation, wrong ETag: {read:?}"
    );

    let read = store
        .get_pinned("o/k", GetRange::Full, &live)
        .await
        .expect("live generation, its own ETag");
    assert_eq!(read.outcome.data, bytes);

    // Identical bytes keep the ETag, so only the generation is stale.
    store
        .put("o/k", bytes.clone(), PutOptions::default())
        .await
        .expect("identical rewrite");
    let (_, replaced) = store.pin_of("o/k").await.expect("pin_of");
    assert_eq!(replaced.etag, live.etag);
    assert_ne!(replaced.version, live.version);
    let read = store.get_pinned("o/k", GetRange::Full, &live).await;
    assert!(
        matches!(read, Err(StoreError::NotFound)),
        "replaced generation, its stale ETag: {read:?}"
    );

    // Different bytes move the ETag too: the old pin's generation and its ETag
    // are both stale, and the generation is decided first.
    store
        .put(
            "o/k",
            Bytes::from_static(b"other bytes"),
            PutOptions::default(),
        )
        .await
        .expect("rewrite with other bytes");
    let (_, rewritten) = store.pin_of("o/k").await.expect("pin_of");
    assert_ne!(rewritten.etag, replaced.etag);
    let read = store.get_pinned("o/k", GetRange::Full, &replaced).await;
    assert!(
        matches!(read, Err(StoreError::NotFound)),
        "replaced generation, a different stale ETag: {read:?}"
    );
}
