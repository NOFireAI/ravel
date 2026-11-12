use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use proptest::prelude::*;

use super::fake::{FakeAzure, HnsAnswer, RecordedRequest};
use super::list::{
    ListFetch, ListRequest, ListResponse, decode_encoded_name, list_delimited, list_page,
    parse_list, parse_xml,
};
use super::{
    AzureConfig, AzureStore, CREDENTIAL_ENV_KEYS, CredentialEnv, SUFFIX_ROUNDS, store_authority,
};
use crate::conformance::{Property, run_conformance_suite_with_seeder};
use crate::s3::SigningClock;
use crate::{
    Etag, GetRange, InstrumentedStore, ObjectMeta, ObjectStoreBackend, PageToken, Pin, PutMode,
    PutOptions, StoreError, StoreMetrics, StoreOp, Version, is_addressable_key,
};

const ACCOUNT: &str = "ravelfake";
const CONTAINER: &str = "ravel-fake";

async fn fake() -> FakeAzure {
    FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("bind the fake")
}

async fn store(fake: &FakeAzure) -> AzureStore {
    AzureStore::new(fake.config())
        .await
        .expect("store for the fake")
}

async fn paged_store(fake: &FakeAzure, page_size: usize) -> AzureStore {
    let mut config = fake.config();
    config.list_page_size = page_size;
    AzureStore::new(config).await.expect("paged store")
}

fn create() -> PutOptions {
    PutOptions {
        mode: PutMode::CreateIfAbsent,
        checksum: None,
    }
}

fn cas(version: &Version) -> PutOptions {
    PutOptions {
        mode: PutMode::CasVersion(version.clone()),
        checksum: None,
    }
}

/// Requests that addressed the blob `key`, in arrival order.
fn blob_requests(fake: &FakeAzure, key: &str) -> Vec<RecordedRequest> {
    fake.requests()
        .into_iter()
        .filter(|r| r.is_blob(ACCOUNT, CONTAINER, key))
        .collect()
}

fn methods(requests: &[RecordedRequest]) -> Vec<&str> {
    requests.iter().map(|r| r.method.as_str()).collect()
}

fn range_header(request: &RecordedRequest) -> Option<&str> {
    request
        .header("x-ms-range")
        .or_else(|| request.header("range"))
}

fn list_requests(fake: &FakeAzure) -> Vec<RecordedRequest> {
    fake.requests()
        .into_iter()
        .filter(|r| r.is_list())
        .collect()
}

/// A base64 shared key, as the portal shows one.
fn shared_key(seed: u8) -> String {
    base64::engine::general_purpose::STANDARD.encode([seed; 64])
}

struct FixedClock(i64);

impl SigningClock for FixedClock {
    fn now_unix_secs(&self) -> i64 {
        self.0
    }
}

#[tokio::test]
async fn conformance_suite_passes_against_the_fake() {
    let fake = fake().await;
    let store = paged_store(&fake, 2).await;

    let report =
        run_conformance_suite_with_seeder(&store, "sys/qualify/fake-azure/", 2, Some(&fake)).await;

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
async fn suffix_read_never_sends_a_suffix_range() {
    let fake = fake().await;
    let store = store(&fake).await;
    let blob = Bytes::from_static(b"0123456789");
    store
        .put("s/k", blob.clone(), PutOptions::default())
        .await
        .expect("put");
    let etag = fake.etag_of("s/k").expect("stored");

    // The fake answers a suffix range as Azure does, with 200 and the whole
    // blob, so a client that sent one and trusted the body would read the
    // wrong bytes.
    let raw = reqwest::Client::new()
        .get(format!("{}/{CONTAINER}/s/k", fake.endpoint()))
        .header("range", "bytes=-4")
        .send()
        .await
        .expect("raw suffix GET");
    assert_eq!(raw.status().as_u16(), 200);
    assert_eq!(raw.bytes().await.expect("body"), blob);

    for (n, expected) in [
        (4, &b"6789"[..]),
        (10, &b"0123456789"[..]),
        (25, &b"0123456789"[..]),
    ] {
        fake.clear_requests();
        let read = store
            .get("s/k", GetRange::Suffix(n))
            .await
            .expect("suffix read");
        assert_eq!(read.data, Bytes::from_static(expected), "n = {n}");
        assert_eq!(read.total_size, 10, "n = {n}");
        assert_eq!(read.etag.0, etag, "n = {n}");

        let requests = blob_requests(&fake, "s/k");
        assert_eq!(methods(&requests), ["HEAD", "GET"], "n = {n}");
        let get = &requests[1];
        let start = 10u64.saturating_sub(n);
        assert_eq!(
            range_header(get),
            Some(format!("bytes={start}-9").as_str()),
            "n = {n}"
        );
        assert_eq!(get.header("if-match"), Some(etag.as_str()), "n = {n}");
        for request in &requests {
            assert!(
                range_header(request).is_none_or(|range| !range.starts_with("bytes=-")),
                "n = {n}: {request:?}"
            );
        }
    }
}

#[tokio::test]
async fn suffix_read_of_an_empty_blob_and_a_zero_suffix_are_invalid_ranges() {
    let fake = fake().await;
    let store = store(&fake).await;
    store
        .put("s/empty", Bytes::new(), PutOptions::default())
        .await
        .expect("put");
    fake.clear_requests();
    let read = store.get("s/empty", GetRange::Suffix(4)).await;
    assert!(matches!(read, Err(StoreError::InvalidRange(_))), "{read:?}");
    assert_eq!(methods(&blob_requests(&fake, "s/empty")), ["HEAD"]);

    fake.clear_requests();
    let read = store.get("s/empty", GetRange::Suffix(0)).await;
    assert!(matches!(read, Err(StoreError::InvalidRange(_))), "{read:?}");
    let read = store.get("s/empty", GetRange::Range(3, 3)).await;
    assert!(matches!(read, Err(StoreError::InvalidRange(_))), "{read:?}");
    assert!(fake.requests().is_empty(), "refused before any request");

    // A range past the end of a non-empty blob is the service's 416.
    store
        .put("s/k", Bytes::from_static(b"abc"), PutOptions::default())
        .await
        .expect("put");
    let read = store.get("s/k", GetRange::Range(5, 9)).await;
    assert!(matches!(read, Err(StoreError::InvalidRange(_))), "{read:?}");
}

#[tokio::test]
async fn a_suffix_read_raced_by_an_overwrite_retries_and_returns_the_new_suffix() {
    let fake = fake().await;
    let store = store(&fake).await;
    store
        .put(
            "s/k",
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let old_etag = fake.etag_of("s/k").expect("stored");
    fake.replace_after_head("s/k", Bytes::from_static(b"abcdefghijklmnop"));
    fake.clear_requests();

    let read = store
        .get_with_pin("s/k", GetRange::Suffix(4))
        .await
        .expect("the second round reads the new blob");

    let new_etag = fake.etag_of("s/k").expect("replaced");
    assert_ne!(new_etag, old_etag);
    assert_eq!(read.outcome.data, Bytes::from_static(b"mnop"));
    assert_eq!(read.outcome.total_size, 16);
    assert_eq!(read.outcome.etag.0, new_etag);
    assert_eq!(read.outcome.version.0, new_etag);
    assert_eq!(read.pin.etag, new_etag);
    let requests = blob_requests(&fake, "s/k");
    assert_eq!(methods(&requests), ["HEAD", "GET", "HEAD", "GET"]);
    assert_eq!(requests[1].header("if-match"), Some(old_etag.as_str()));
    assert_eq!(range_header(&requests[1]), Some("bytes=6-9"));
    assert_eq!(requests[3].header("if-match"), Some(new_etag.as_str()));
    assert_eq!(range_header(&requests[3]), Some("bytes=12-15"));
}

#[tokio::test]
async fn a_suffix_read_replaced_every_round_is_transient_after_three_rounds() {
    let fake = fake().await;
    let store = store(&fake).await;
    store
        .put(
            "s/k",
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put");
    for round in 0..SUFFIX_ROUNDS + 1 {
        fake.replace_after_head("s/k", Bytes::from(format!("replacement {round}")));
    }
    fake.clear_requests();

    let read = store.get("s/k", GetRange::Suffix(4)).await;

    assert!(matches!(read, Err(StoreError::Transient(_))), "{read:?}");
    assert_eq!(SUFFIX_ROUNDS, 3);
    assert_eq!(
        methods(&blob_requests(&fake, "s/k")),
        ["HEAD", "GET", "HEAD", "GET", "HEAD", "GET"]
    );
}

#[tokio::test]
async fn a_pinned_suffix_read_after_a_replace_fails_in_one_round() {
    let fake = fake().await;
    let store = store(&fake).await;
    store
        .put(
            "s/k",
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let (_, pin) = store.pin_of("s/k").await.expect("pin_of");
    store
        .put(
            "s/k",
            Bytes::from_static(b"replaced"),
            PutOptions::default(),
        )
        .await
        .expect("replace");
    fake.clear_requests();

    let read = store.get_pinned("s/k", GetRange::Suffix(4), &pin).await;

    assert!(
        matches!(read, Err(StoreError::PreconditionFailed)),
        "{read:?}"
    );
    let requests = blob_requests(&fake, "s/k");
    assert_eq!(methods(&requests), ["HEAD"]);
    assert_eq!(requests[0].header("if-match"), Some(pin.etag.as_str()));

    // A pinned suffix of the live blob conditions the HEAD and the GET on it.
    let (_, live) = store.pin_of("s/k").await.expect("pin_of");
    fake.clear_requests();
    let read = store
        .get_pinned("s/k", GetRange::Suffix(3), &live)
        .await
        .expect("pinned suffix of the live blob");
    assert_eq!(read.outcome.data, Bytes::from_static(b"ced"));
    let requests = blob_requests(&fake, "s/k");
    assert_eq!(methods(&requests), ["HEAD", "GET"]);
    for request in &requests {
        assert_eq!(request.header("if-match"), Some(live.etag.as_str()));
    }
}

#[tokio::test]
async fn a_pin_with_a_version_selects_it_on_the_head_and_the_get() {
    let fake = fake().await;
    let store = store(&fake).await;
    store
        .put(
            "s/k",
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let etag = fake.etag_of("s/k").expect("stored");
    let pin = Pin::from_store(etag, Some("2026-11-12T00:00:00.0000000Z".into()));
    fake.clear_requests();

    // The fake keeps no versions, so the selector is a 404 on the HEAD.
    let read = store.get_pinned("s/k", GetRange::Suffix(4), &pin).await;
    assert!(matches!(read, Err(StoreError::NotFound)), "{read:?}");
    let requests = blob_requests(&fake, "s/k");
    assert_eq!(methods(&requests), ["HEAD"]);
    assert_eq!(
        requests[0].query_param("versionid"),
        Some("2026-11-12T00:00:00.0000000Z")
    );

    fake.clear_requests();
    let read = store.get_pinned("s/k", GetRange::Full, &pin).await;
    assert!(matches!(read, Err(StoreError::NotFound)), "{read:?}");
    let requests = blob_requests(&fake, "s/k");
    assert_eq!(methods(&requests), ["GET"]);
    assert_eq!(
        requests[0].query_param("versionid"),
        Some("2026-11-12T00:00:00.0000000Z")
    );
}

#[tokio::test]
async fn each_put_mode_sends_its_precondition() {
    let fake = fake().await;
    let store = store(&fake).await;

    let created = store
        .put("p/k", Bytes::from_static(b"one"), create())
        .await
        .expect("create");
    let overwritten = store
        .put("p/k", Bytes::from_static(b"two"), PutOptions::default())
        .await
        .expect("overwrite");
    let swapped = store
        .put(
            "p/k",
            Bytes::from_static(b"three"),
            cas(&overwritten.version),
        )
        .await
        .expect("cas");

    let puts: Vec<_> = blob_requests(&fake, "p/k")
        .into_iter()
        .filter(|r| r.method == "PUT")
        .collect();
    assert_eq!(puts.len(), 3);
    assert_eq!(puts[0].header("if-none-match"), Some("*"));
    assert_eq!(puts[0].header("if-match"), None);
    assert_eq!(puts[1].header("if-none-match"), None);
    assert_eq!(puts[1].header("if-match"), None);
    assert_eq!(puts[2].header("if-none-match"), None);
    assert_eq!(
        puts[2].header("if-match"),
        Some(overwritten.version.0.as_str())
    );
    for put in &puts {
        assert_eq!(put.header("x-ms-blob-type"), Some("BlockBlob"));
    }

    // The version token is the ETag, quotes included, on every path.
    assert_eq!(created.etag.0, created.version.0);
    assert!(created.etag.0.starts_with('"'), "{created:?}");
    assert_eq!(fake.etag_of("p/k"), Some(swapped.etag.0.clone()));
    let head = store.head("p/k").await.expect("head");
    assert_eq!(head.version, swapped.version);
    assert_eq!(head.etag, swapped.etag);
    let read = store.get("p/k", GetRange::Full).await.expect("get");
    assert_eq!(read.version, swapped.version);
    assert_eq!(read.data, Bytes::from_static(b"three"));

    let stale = store
        .put("p/k", Bytes::from_static(b"four"), cas(&created.version))
        .await;
    assert!(
        matches!(stale, Err(StoreError::PreconditionFailed)),
        "{stale:?}"
    );
    let again = store
        .put("p/k", Bytes::from_static(b"four"), create())
        .await;
    assert!(matches!(again, Err(StoreError::AlreadyExists)), "{again:?}");
    let absent = store
        .put("p/absent", Bytes::from_static(b"x"), cas(&created.version))
        .await;
    assert!(
        matches!(absent, Err(StoreError::PreconditionFailed)),
        "{absent:?}"
    );
}

#[tokio::test]
async fn error_rows_follow_the_adr() {
    let fake = fake().await;
    let store = store(&fake).await;

    // HEAD on a missing blob is NotFound; on a missing container, Permanent.
    let missing = store.head("e/absent").await;
    assert!(matches!(missing, Err(StoreError::NotFound)), "{missing:?}");
    let other = AzureStore::new(fake.config_for_container("absent-container"))
        .await
        .expect("account information does not need the container");
    let head = other.head("e/k").await;
    assert!(matches!(head, Err(StoreError::Permanent(_))), "{head:?}");
    let get = other.get("e/k", GetRange::Full).await;
    assert!(matches!(get, Err(StoreError::Permanent(_))), "{get:?}");
    let suffix = other.get("e/k", GetRange::Suffix(2)).await;
    assert!(
        matches!(suffix, Err(StoreError::Permanent(_))),
        "{suffix:?}"
    );
    let put = other
        .put("e/k", Bytes::from_static(b"x"), PutOptions::default())
        .await;
    assert!(matches!(put, Err(StoreError::Permanent(_))), "{put:?}");
    let cas_put = other
        .put(
            "e/k",
            Bytes::from_static(b"x"),
            cas(&Version("\"0x1\"".into())),
        )
        .await;
    assert!(
        matches!(cas_put, Err(StoreError::Permanent(_))),
        "{cas_put:?}"
    );
    let delete = other.delete("e/k").await;
    assert!(
        matches!(delete, Err(StoreError::Permanent(_))),
        "{delete:?}"
    );
    let list = other.list("e/", None).await;
    assert!(matches!(list, Err(StoreError::Permanent(_))), "{list:?}");
    let delimited = other.list_delimited("e/").await;
    assert!(
        matches!(delimited, Err(StoreError::Permanent(_))),
        "{delimited:?}"
    );
    let pinned = other.pin_of("e/k").await;
    assert!(
        matches!(pinned, Err(StoreError::Permanent(_))),
        "{pinned:?}"
    );

    // A 404 BlobNotFound on a CasVersion put is PreconditionFailed: the key is
    // gone, so the token no longer names it.
    fake.fail_next_with_code(404, "BlobNotFound", 1);
    fake.clear_requests();
    let gone = store
        .put(
            "e/k",
            Bytes::from_static(b"x"),
            cas(&Version("\"0x1\"".into())),
        )
        .await;
    assert!(
        matches!(gone, Err(StoreError::PreconditionFailed)),
        "{gone:?}"
    );
    assert_eq!(methods(&blob_requests(&fake, "e/k")), ["PUT"]);

    // A 409 on a create that is not BlobAlreadyExists is not AlreadyExists.
    fake.fail_next_with_code(409, "LeaseIdMissing", 1);
    let leased = store.put("e/k", Bytes::from_static(b"x"), create()).await;
    assert!(
        matches!(leased, Err(StoreError::Permanent(_))),
        "{leased:?}"
    );
    fake.fail_next(409, 1);
    let overwrite = store
        .put("e/k", Bytes::from_static(b"x"), PutOptions::default())
        .await;
    assert!(
        matches!(overwrite, Err(StoreError::Permanent(_))),
        "{overwrite:?}"
    );

    // A create 409 with no code is settled with one HEAD.
    fake.fail_next_without_code(409, 1);
    fake.clear_requests();
    let unsettled = store.put("e/k", Bytes::from_static(b"x"), create()).await;
    assert!(
        matches!(unsettled, Err(StoreError::Transient(_))),
        "absent on the HEAD: {unsettled:?}"
    );
    assert_eq!(methods(&blob_requests(&fake, "e/k")), ["PUT", "HEAD"]);
    store
        .put("e/k", Bytes::from_static(b"x"), PutOptions::default())
        .await
        .expect("put");
    fake.fail_next_without_code(409, 1);
    let present = store.put("e/k", Bytes::from_static(b"y"), create()).await;
    assert!(
        matches!(present, Err(StoreError::AlreadyExists)),
        "present on the HEAD: {present:?}"
    );

    // 401 and 403 are AccessDenied; 416 InvalidRange.
    for status in [401, 403] {
        fake.fail_next(status, 1);
        let denied = store.get("e/k", GetRange::Full).await;
        assert!(
            matches!(denied, Err(StoreError::AccessDenied(_))),
            "{status}: {denied:?}"
        );
        fake.fail_next(status, 1);
        let denied = store.head("e/k").await;
        assert!(
            matches!(denied, Err(StoreError::AccessDenied(_))),
            "{status} on HEAD: {denied:?}"
        );
    }

    // 429, 503 and 500 are retried; past the retry budget they surface as
    // retryable errors.
    for status in [429, 500, 503] {
        fake.fail_next(status, 1);
        store
            .get("e/k", GetRange::Full)
            .await
            .expect("one failure is retried");
        fake.fail_next(status, 1);
        store.head("e/k").await.expect("one failure is retried");
        fake.fail_next(status, 3);
        let exhausted = store.head("e/k").await;
        assert!(
            exhausted.as_ref().is_err_and(StoreError::is_retryable),
            "{status}: {exhausted:?}"
        );
    }

    // Deleting a missing blob succeeds.
    store.delete("e/absent").await.expect("idempotent delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_have_exactly_one_winner() {
    let fake = fake().await;
    let store = Arc::new(store(&fake).await);
    let writers = (0..8u8).map(|writer| {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .put("c/k", Bytes::from(vec![writer; 16]), create())
                .await
        })
    });
    let outcomes: Vec<_> = futures::future::join_all(writers)
        .await
        .into_iter()
        .map(|joined| joined.expect("writer task"))
        .collect();

    let winners: Vec<_> = outcomes.iter().filter(|o| o.is_ok()).collect();
    let losers = outcomes
        .iter()
        .filter(|o| matches!(o, Err(StoreError::AlreadyExists)))
        .count();
    assert_eq!(winners.len(), 1, "{outcomes:?}");
    assert_eq!(losers, 7, "{outcomes:?}");
    let stored = fake.data_of("c/k").expect("the winner's bytes");
    assert_eq!(
        winners[0].as_ref().map(|w| w.etag.0.clone()).ok(),
        fake.etag_of("c/k")
    );
    assert!(stored.iter().all(|b| *b == stored[0]), "one writer's bytes");
}

async fn seed(store: &AzureStore, keys: &[&str]) {
    for key in keys {
        store
            .put(key, Bytes::from(key.to_string()), PutOptions::default())
            .await
            .expect("seed");
    }
}

async fn keys_after(store: &AzureStore, prefix: &str, after: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut token = None;
    for _ in 0..16 {
        let page = store
            .list_after(prefix, Some(after), token)
            .await
            .expect("list_after");
        keys.extend(page.objects.into_iter().map(|o| o.key));
        match page.next {
            Some(next) => token = Some(next),
            None => return keys,
        }
    }
    panic!("list_after did not finish in 16 pages");
}

#[tokio::test]
async fn list_after_is_exclusive_whether_or_not_start_from_is_honoured() {
    let fake = fake().await;
    let keys = ["l/a", "l/b", "l/c", "l/d"];
    for page_size in [1000, 2] {
        let store = paged_store(&fake, page_size).await;
        seed(&store, &keys).await;
        for honour in [true, false] {
            fake.set_honour_start_from(honour);
            fake.clear_requests();
            assert_eq!(
                keys_after(&store, "l/", "l/b").await,
                ["l/c", "l/d"],
                "page size {page_size}, startFrom honoured: {honour}"
            );
            let first = &list_requests(&fake)[0];
            assert_eq!(first.query_param("startFrom"), Some("l/b"));
            assert_eq!(first.query_param("prefix"), Some("l/"));
            assert_eq!(
                first.query_param("maxresults"),
                Some(page_size.to_string().as_str())
            );
            assert_eq!(first.query_param("delimiter"), None);
        }
    }
    fake.set_honour_start_from(true);
}

#[tokio::test]
async fn a_page_token_is_the_raw_last_key_and_resumes_after_it() {
    let fake = fake().await;
    let store = paged_store(&fake, 2).await;
    seed(&store, &["t/a", "t/b", "t/c"]).await;
    let page = store.list("t/", None).await.expect("first page");
    assert_eq!(page.next, Some(PageToken("t/b".into())));
    fake.clear_requests();
    let page = store
        .list("t/", Some(PageToken("t/b".into())))
        .await
        .expect("second page");
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["t/c"]);
    assert_eq!(page.next, None);
    assert_eq!(
        list_requests(&fake)[0].query_param("startFrom"),
        Some("t/b")
    );

    // A token takes precedence over list_after's start key.
    let page = store
        .list_after("t/", Some("t/a"), Some(PageToken("t/b".into())))
        .await
        .expect("token first");
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["t/c"]);
}

#[tokio::test]
async fn page_size_two_lists_every_key_in_order() {
    let fake = fake().await;
    let store = paged_store(&fake, 2).await;
    let mut keys: Vec<String> = (0..11).map(|i| format!("o/{i:02}")).collect();
    keys.push("o/a b+c".into());
    keys.push("o/z.y_-".into());
    for key in &keys {
        store
            .put(key, Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("put");
    }
    fake.set_honour_start_from(false);
    for encode in [false, true] {
        fake.set_encode_all_names(encode);
        let mut listed = Vec::new();
        let mut token = None;
        let mut pages = 0;
        loop {
            let page = store.list("o/", token).await.expect("list");
            pages += 1;
            assert!(page.objects.len() <= 2, "{page:?}");
            listed.extend(page.objects.into_iter().map(|o| o.key));
            match page.next {
                Some(next) => token = Some(next),
                None => break,
            }
            assert!(pages < 32, "listing did not end");
        }
        assert!(
            listed.windows(2).all(|w| w[0] <= w[1]),
            "encoded {encode}: {listed:?}"
        );
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(listed, expected, "encoded {encode}");
    }
}

#[tokio::test]
async fn encoded_and_unaddressable_names_are_listed_exactly() {
    let fake = fake().await;
    let store = store(&fake).await;
    fake.insert_blob("n/dir/a b+c", Bytes::from_static(b"1"));
    fake.insert_blob("n/x\u{1}y", Bytes::from_static(b"2"));
    fake.insert_blob("n/a//b", Bytes::from_static(b"3"));

    for encode in [false, true] {
        fake.set_encode_all_names(encode);
        let page = store.list("n/", None).await.expect("list is not fatal");
        let mut all: Vec<String> = page.objects.iter().map(|o| o.key.clone()).collect();
        all.extend(page.unaddressable.iter().map(|u| u.key.clone()));
        all.sort();
        assert_eq!(
            all,
            ["n/a//b", "n/dir/a b+c", "n/x\u{1}y"],
            "encoded {encode}"
        );
        for object in &page.objects {
            assert!(is_addressable_key(&object.key), "{object:?}");
        }
        for unaddressable in &page.unaddressable {
            assert!(!is_addressable_key(&unaddressable.key), "{unaddressable:?}");
        }
        assert!(
            page.unaddressable.iter().any(|u| u.key == "n/a//b"),
            "{page:?}"
        );
        assert!(
            page.objects.iter().any(|o| o.key == "n/dir/a b+c"),
            "{page:?}"
        );

        let delimited = store.list_delimited("n/").await.expect("delimited");
        assert_eq!(
            delimited.common_prefixes,
            ["n/a/", "n/dir/"],
            "encoded {encode}"
        );
    }
    // The listed name is the stored one: an encoded slash decodes to a slash.
    let head = store.head("n/dir/a b+c").await.expect("head");
    assert_eq!(head.size, 1);
}

#[tokio::test]
async fn list_and_head_report_the_same_etag() {
    let fake = fake().await;
    let store = paged_store(&fake, 2).await;
    seed(&store, &["h/a", "h/b", "h/c"]).await;
    let mut token = None;
    let mut seen = 0;
    loop {
        let page = store.list("h/", token).await.expect("list");
        for listed in &page.objects {
            let head = store.head(&listed.key).await.expect("head");
            assert_eq!(listed.etag, head.etag, "{}", listed.key);
            assert_eq!(listed.version, head.version, "{}", listed.key);
            assert_eq!(listed.size, head.size, "{}", listed.key);
            assert_eq!(
                listed.last_modified_unix_ms, head.last_modified_unix_ms,
                "{}",
                listed.key
            );
            seen += 1;
        }
        match page.next {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, 3);
}

/// Plays back canned List Blobs responses, then empty final ones, and
/// records the marker each request carried.
struct ScriptedFetch {
    responses: parking_lot::Mutex<std::collections::VecDeque<ListResponse>>,
    markers: parking_lot::Mutex<Vec<Option<String>>>,
}

impl ScriptedFetch {
    fn new(responses: Vec<ListResponse>) -> Self {
        ScriptedFetch {
            responses: parking_lot::Mutex::new(responses.into()),
            markers: parking_lot::Mutex::default(),
        }
    }

    fn requests(&self) -> usize {
        self.markers.lock().len()
    }
}

impl ListFetch for ScriptedFetch {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
        self.markers.lock().push(request.marker.map(str::to_string));
        Ok(self.responses.lock().pop_front().unwrap_or_default())
    }
}

/// A response listing `names`, continued by `next` when it is `Some`.
fn response(names: &[&str], next: Option<&str>) -> ListResponse {
    ListResponse {
        items: names
            .iter()
            .map(|name| ObjectMeta {
                key: (*name).to_string(),
                size: 1,
                etag: Etag("\"0x1\"".into()),
                version: Version("\"0x1\"".into()),
                last_modified_unix_ms: 0,
            })
            .collect(),
        prefixes: Vec::new(),
        next_marker: next.map(str::to_string),
    }
}

#[tokio::test]
async fn a_repeated_marker_fails_the_listing() {
    let script = || {
        ScriptedFetch::new(vec![
            response(&["p/a"], Some("m1")),
            response(&["p/b"], Some("m1")),
        ])
    };
    let fetch = script();
    let page = list_page(&fetch, "p/", None, 10).await;
    assert!(
        matches!(page, Err(StoreError::ListRepeatedToken { .. })),
        "{page:?}"
    );
    assert_eq!(*fetch.markers.lock(), [None, Some("m1".to_string())]);

    let fetch = script();
    let drained = list_delimited(&fetch, "p/", crate::MAX_LIST_PAGES).await;
    assert!(
        matches!(drained, Err(StoreError::ListRepeatedToken { .. })),
        "{drained:?}"
    );
    assert_eq!(fetch.requests(), 2);
}

#[tokio::test]
async fn sixteen_empty_continued_responses_fail_the_listing() {
    let empty: Vec<ListResponse> = (0..32)
        .map(|i| response(&[], Some(&format!("m{i}"))))
        .collect();
    let fetch = ScriptedFetch::new(empty);
    let page = list_page(&fetch, "p/", None, 10).await;
    assert!(
        matches!(page, Err(StoreError::ListPageCeiling { ceiling: 16, .. })),
        "{page:?}"
    );
    assert_eq!(fetch.requests(), 16);

    // A response whose names the resume filter dropped lists something, so it
    // does not count: a service ignoring startFrom costs requests only.
    let mut ignored: Vec<ListResponse> = (0..20)
        .map(|i| response(&["p/a"], Some(&format!("m{i}"))))
        .collect();
    ignored.push(response(&["p/z"], None));
    let fetch = ScriptedFetch::new(ignored);
    let page = list_page(&fetch, "p/", Some("p/m"), 10)
        .await
        .expect("filtered responses are not empty ones");
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["p/z"]);
    assert_eq!(page.next, None);
    assert_eq!(fetch.requests(), 21);
}

#[tokio::test]
async fn a_response_the_filter_trimmed_ends_the_call_without_a_second_request() {
    let fetch = ScriptedFetch::new(vec![
        response(&["p/a", "p/b", "p/c"], Some("m1")),
        response(&["p/d"], None),
    ]);
    let page = list_page(&fetch, "p/", Some("p/b"), 10)
        .await
        .expect("list");
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["p/c"]);
    assert_eq!(page.next, Some(PageToken("p/c".into())));
    assert_eq!(fetch.requests(), 1);
}

#[tokio::test]
async fn delimited_listing_sends_the_wire_maximum() {
    let fake = fake().await;
    let store = paged_store(&fake, 2).await;
    seed(&store, &["d/a", "d/b/1", "d/b/2", "d/c/1", "d/e"]).await;
    fake.clear_requests();
    let listed = store.list_delimited("d/").await.expect("delimited");
    let keys: Vec<_> = listed.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["d/a", "d/e"]);
    assert_eq!(listed.common_prefixes, ["d/b/", "d/c/"]);
    let requests = list_requests(&fake);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].query_param("maxresults"), Some("1000"));
    assert_eq!(requests[0].query_param("delimiter"), Some("/"));
}

#[tokio::test]
async fn a_hierarchical_namespace_account_is_refused_at_construction() {
    let fake = fake().await;
    for (answer, needle) in [
        (HnsAnswer::True, "hierarchical namespace enabled"),
        (HnsAnswer::Absent, "carried no x-ms-is-hns-enabled"),
        (HnsAnswer::Unreadable, "unreadable"),
    ] {
        fake.set_hns(answer);
        fake.clear_requests();
        let built = AzureStore::new(fake.config()).await;
        match built {
            Err(StoreError::Permanent(message)) => {
                assert!(message.contains(needle), "{answer:?}: {message}");
                assert!(message.contains(ACCOUNT), "{answer:?}: {message}");
            }
            other => panic!("{answer:?}: {other:?}"),
        }
        let requests = fake.requests();
        assert_eq!(requests.len(), 1, "{answer:?}");
        assert_eq!(requests[0].query_param("restype"), Some("account"));
        assert_eq!(requests[0].query_param("comp"), Some("properties"));
    }
    fake.set_hns(HnsAnswer::False);
    AzureStore::new(fake.config())
        .await
        .expect("a flat account is accepted");
}

#[test]
fn credentials_are_read_from_the_five_keys_only() {
    let asked = parking_lot::Mutex::new(Vec::new());
    let environment = [
        ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
        ("AZURE_CLIENT_ID", "client"),
        ("AZURE_TENANT_ID", "tenant"),
        ("AZURE_AUTHORITY_HOST", ""),
        ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
        ("AZURE_STORAGE_SAS_KEY", "sv=stray"),
        ("AZURE_CLIENT_SECRET", "stray"),
        ("AZURE_USE_AZURE_CLI", "true"),
        ("AZURE_ENDPOINT", "http://stray.example"),
        ("AZURE_STORAGE_ACCOUNT_NAME", "stray"),
    ];
    let env = CredentialEnv::from_lookup(|name| {
        asked.lock().push(name.to_string());
        environment
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_string())
            .ok_or(std::env::VarError::NotPresent)
    })
    .expect("read");

    let mut asked = asked.into_inner();
    asked.sort();
    let mut expected: Vec<String> = CREDENTIAL_ENV_KEYS.iter().map(|k| k.to_string()).collect();
    expected.sort();
    assert_eq!(asked, expected);
    assert_eq!(
        env,
        CredentialEnv {
            federated_token_file: Some("/var/run/token".into()),
            client_id: Some("client".into()),
            tenant_id: Some("tenant".into()),
            authority_host: None,
            account_key: Some("a2V5".into()),
        }
    );
    let printed = format!("{env:?}");
    assert!(!printed.contains("a2V5"), "{printed}");

    let not_unicode = CredentialEnv::from_lookup(|name| {
        if name == "AZURE_CLIENT_ID" {
            Err(std::env::VarError::NotUnicode("\u{fffd}".into()))
        } else {
            Err(std::env::VarError::NotPresent)
        }
    });
    match not_unicode {
        Err(StoreError::Permanent(message)) => assert!(message.contains("AZURE_CLIENT_ID")),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn attempts_and_observed_store_time_are_recorded() {
    let fake = fake().await;
    let metrics = Arc::new(StoreMetrics::default());
    let store = InstrumentedStore::with_metrics(
        AzureStore::with_metrics(fake.config(), Arc::clone(&metrics))
            .await
            .expect("store"),
        Arc::clone(&metrics),
    );
    // Construction's Get Account Information response is observed, outside
    // any operation, so it is no attempt.
    assert_eq!(store.observed_store_time_ns(), Some(fake.date_unix_ns()));
    let before = metrics.snapshot();
    for op in [StoreOp::Get, StoreOp::Head, StoreOp::List] {
        assert_eq!(before.op(op).attempts, 0, "{op:?}");
    }
    fake.set_date_unix_secs(1_800_000_000);
    fake.clear_requests();

    fake.fail_next(503, 1);
    store
        .put("m/k", Bytes::from_static(b"value"), PutOptions::default())
        .await
        .expect("one 503 is retried");
    assert_eq!(store.observed_store_time_ns(), Some(fake.date_unix_ns()));

    fake.set_date_unix_secs(1_800_000_060);
    store.get("m/k", GetRange::Full).await.expect("get");
    store.get("m/k", GetRange::Suffix(2)).await.expect("suffix");
    store.head("m/k").await.expect("head");
    store.list("m/", None).await.expect("list");
    store.list_delimited("m/").await.expect("list_delimited");
    store.delete("m/k").await.expect("delete");
    assert_eq!(store.observed_store_time_ns(), Some(fake.date_unix_ns()));
    assert_eq!(fake.date_unix_ns(), 1_800_000_060_000_000_000);

    let snapshot = metrics.snapshot();
    let attempts = |op| snapshot.op(op).attempts;
    let calls = |op| snapshot.op(op).calls;
    assert_eq!(attempts(StoreOp::Put), 2, "the 503 and the retry");
    assert_eq!(calls(StoreOp::Put), 1);
    assert_eq!(
        attempts(StoreOp::Get),
        3,
        "a full GET, and a HEAD and a GET"
    );
    assert_eq!(calls(StoreOp::Get), 2);
    assert_eq!(attempts(StoreOp::Head), 1);
    assert_eq!(attempts(StoreOp::List), 1);
    assert_eq!(attempts(StoreOp::ListDelimited), 1);
    assert_eq!(attempts(StoreOp::Delete), 1);
    assert_eq!(fake.requests().len(), 9, "every attempt counted, no more");
    assert_eq!(metrics.get_unverified(), 1, "the full read only");
}

/// A stand-in Microsoft Entra ID token endpoint on its own loopback port: it
/// answers every request with a bearer token that expires at once, so the
/// store fetches a fresh one inside every operation, and with a `Date` far
/// from the fake's. The number of requests it served.
async fn token_endpoint() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let app = axum::Router::new().fallback(move || {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (
                [
                    ("content-type", "application/json"),
                    ("date", "Mon, 01 Jan 2001 00:00:00 GMT"),
                ],
                r#"{"access_token":"workload-token","token_type":"Bearer","expires_in":0}"#,
            )
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the token endpoint");
    let addr = listener.local_addr().expect("token endpoint address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn workload_identity_tokens_are_fetched_from_another_host_uncounted() {
    let fake = fake().await;
    let (authority_host, token_requests) = token_endpoint().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, "service-account-jwt").expect("write the token file");
    let env = CredentialEnv {
        federated_token_file: Some(token_file.to_string_lossy().into_owned()),
        client_id: Some("client".into()),
        tenant_id: Some("tenant".into()),
        authority_host: Some(authority_host),
        account_key: None,
    };
    let mut config = fake.config();
    config.skip_signature = false;
    let metrics = Arc::new(StoreMetrics::default());
    let store = AzureStore::build(
        config,
        Arc::clone(&metrics),
        env,
        Arc::new(FixedClock(1_800_000_000)),
    )
    .await
    .expect("a store authenticated by workload identity");
    let fetched_at_construction = token_requests.load(std::sync::atomic::Ordering::SeqCst);
    assert!(fetched_at_construction > 0, "no token was fetched");

    store
        .put("w/k", Bytes::from_static(b"value"), PutOptions::default())
        .await
        .expect("put");
    store.get("w/k", GetRange::Full).await.expect("get");
    store.list("w/", None).await.expect("list");

    assert!(
        token_requests.load(std::sync::atomic::Ordering::SeqCst) > fetched_at_construction,
        "no token was fetched inside an operation, so nothing was excluded"
    );
    let snapshot = metrics.snapshot();
    for op in [StoreOp::Put, StoreOp::Get, StoreOp::List] {
        assert_eq!(
            snapshot.op(op).attempts,
            1,
            "{op:?}: the store request only"
        );
    }
    assert_eq!(store.observed_store_time_ns(), Some(fake.date_unix_ns()));
    let requests = fake.requests();
    assert_eq!(requests.len(), 4, "account information, put, get, list");
    for request in &requests {
        assert_eq!(
            request.header("authorization"),
            Some("Bearer workload-token"),
            "{request:?}"
        );
    }
}

#[tokio::test]
async fn shared_key_requests_verify_and_a_wrong_key_is_access_denied() {
    let fake = fake().await;
    let key = shared_key(7);
    fake.require_shared_key(&key);

    let store = AzureStore::new(fake.config_with_key(&key))
        .await
        .expect("a signed account information request");
    let put = store
        .put("k/a b+c", Bytes::from_static(b"signed"), create())
        .await
        .expect("object_store's signed put");
    store
        .put("k/a b+c", Bytes::from_static(b"again"), cas(&put.version))
        .await
        .expect("a signed If-Match put");
    store.get("k/a b+c", GetRange::Full).await.expect("get");
    let suffix = store
        .get("k/a b+c", GetRange::Suffix(3))
        .await
        .expect("my signed HEAD and object_store's signed ranged GET");
    assert_eq!(suffix.data, Bytes::from_static(b"ain"));
    let (_, pin) = store.pin_of("k/a b+c").await.expect("pin_of");
    store
        .get_pinned("k/a b+c", GetRange::Suffix(3), &pin)
        .await
        .expect("a signed If-Match HEAD");
    let page = store.list("k/", None).await.expect("signed list");
    assert_eq!(page.objects.len(), 1);
    store
        .list_delimited("")
        .await
        .expect("signed delimited list");
    let mut upload = store.put_multipart("k/multi").await.expect("open");
    upload
        .put_part(Bytes::from(vec![1u8; crate::MULTIPART_MIN_PART_SIZE]), None)
        .await
        .expect("signed Put Block");
    upload
        .put_part(Bytes::from_static(b"tail"), None)
        .await
        .expect("signed Put Block");
    upload.complete().await.expect("signed Put Block List");
    store.delete("k/a b+c").await.expect("signed delete");
    let signed = fake.requests();
    assert!(
        signed.iter().all(|r| r
            .header("authorization")
            .is_some_and(|a| a.starts_with("SharedKey "))),
        "{signed:?}"
    );

    // The environment's key is used when the configuration names none.
    let env = CredentialEnv {
        account_key: Some(key.clone()),
        ..CredentialEnv::default()
    };
    let mut config = fake.config();
    config.skip_signature = false;
    let from_env = AzureStore::build(
        config,
        Arc::default(),
        env,
        Arc::new(FixedClock(1_800_000_000)),
    )
    .await
    .expect("the environment's key");
    from_env.head("k/multi").await.expect("signed head");

    let wrong = AzureStore::new(fake.config_with_key(&shared_key(8))).await;
    assert!(
        matches!(wrong, Err(StoreError::AccessDenied(_))),
        "{wrong:?}"
    );

    fake.require_shared_key(&shared_key(9));
    let denied = store
        .put("k/x", Bytes::from_static(b"x"), PutOptions::default())
        .await;
    assert!(
        matches!(denied, Err(StoreError::AccessDenied(_))),
        "object_store's request: {denied:?}"
    );
    let denied = store.head("k/multi").await;
    assert!(
        matches!(denied, Err(StoreError::AccessDenied(_))),
        "my request: {denied:?}"
    );

    let not_base64 = AzureStore::new(fake.config_with_key("not base64!")).await;
    match not_base64 {
        Err(StoreError::Permanent(message)) => {
            assert!(message.contains("AZURE_STORAGE_ACCOUNT_KEY"), "{message}");
            assert!(!message.contains("not base64!"), "{message}");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn multipart_sends_put_block_and_put_block_list_and_abort_sends_nothing() {
    let fake = fake().await;
    let store = store(&fake).await;
    let first = Bytes::from(vec![3u8; crate::MULTIPART_MIN_PART_SIZE]);
    let mut upload = store.put_multipart("u/k").await.expect("open");
    upload.put_part(first.clone(), None).await.expect("part 1");
    upload
        .put_part(Bytes::from_static(b"tail"), None)
        .await
        .expect("part 2");
    let outcome = upload.complete().await.expect("complete");

    let requests = blob_requests(&fake, "u/k");
    let comps: Vec<_> = requests.iter().map(|r| r.query_param("comp")).collect();
    assert_eq!(comps, [Some("block"), Some("block"), Some("blocklist")]);
    assert!(requests[2].header("if-match").is_none(), "unconditional");
    assert!(
        requests[2].header("if-none-match").is_none(),
        "unconditional"
    );
    let mut expected = first.to_vec();
    expected.extend_from_slice(b"tail");
    assert_eq!(fake.data_of("u/k"), Some(Bytes::from(expected)));
    assert_eq!(fake.etag_of("u/k"), Some(outcome.etag.0.clone()));
    assert_eq!(outcome.version.0, outcome.etag.0);
    assert_eq!(fake.uncommitted_blocks(), 0);

    fake.clear_requests();
    let mut upload = store.put_multipart("u/aborted").await.expect("open");
    upload
        .put_part(Bytes::from(vec![4u8; crate::MULTIPART_MIN_PART_SIZE]), None)
        .await
        .expect("part");
    upload.abort().await.expect("abort");
    assert_eq!(methods(&blob_requests(&fake, "u/aborted")), ["PUT"]);
    assert_eq!(fake.data_of("u/aborted"), None);
}

#[tokio::test]
async fn container_not_found_during_a_multipart_upload_is_permanent() {
    let fake = fake().await;
    let other = AzureStore::new(fake.config_for_container("absent-container"))
        .await
        .expect("account information does not need the container");
    let mut upload = other
        .put_multipart("u/k")
        .await
        .expect("open sends nothing");
    let part = upload
        .put_part(Bytes::from(vec![5u8; crate::MULTIPART_MIN_PART_SIZE]), None)
        .await;
    assert!(matches!(part, Err(StoreError::Permanent(_))), "{part:?}");

    let store = store(&fake).await;
    let mut upload = store.put_multipart("u/k").await.expect("open");
    upload
        .put_part(Bytes::from_static(b"only"), None)
        .await
        .expect("part");
    fake.fail_next_with_code(404, "ContainerNotFound", 1);
    let complete = upload.complete().await;
    assert!(
        matches!(complete, Err(StoreError::Permanent(_))),
        "{complete:?}"
    );

    // A 404 without that code during an upload is still NotFound.
    let mut upload = store.put_multipart("u/k").await.expect("open");
    fake.fail_next_with_code(404, "BlobNotFound", 1);
    let part = upload.put_part(Bytes::from_static(b"only"), None).await;
    assert!(matches!(part, Err(StoreError::NotFound)), "{part:?}");
}

#[tokio::test]
async fn capabilities_and_identity() {
    let fake = fake().await;
    let store = store(&fake).await;
    let capabilities = store.capabilities();
    assert!(capabilities.satisfies(&crate::Capabilities::mandatory()));
    assert!(capabilities.multipart);
    assert!(capabilities.suffix_range);
    assert!(!capabilities.upload_checksum);
    assert_eq!(
        store.backend_identity(),
        format!("az://{ACCOUNT}/{CONTAINER}@{}", fake.endpoint())
    );
    assert_eq!(
        AzureConfig::new("acct", "box").backend_identity(),
        "az://acct/box"
    );
}

#[tokio::test]
async fn configuration_is_validated_before_any_request() {
    let fake = fake().await;
    let mut bad = Vec::new();
    let mut config = AzureConfig::new("acct", "box");
    config.allow_http = true;
    bad.push(config);
    let mut config = AzureConfig::new("acct", "box");
    config.skip_signature = true;
    bad.push(config);
    bad.push(AzureConfig::new("Acct", "box"));
    bad.push(AzureConfig::new("acct", "Box"));
    bad.push(AzureConfig::new("", "box"));
    let mut config = fake.config();
    config.list_page_size = 0;
    bad.push(config);
    let mut config = fake.config();
    config.endpoint = Some(format!(
        "http://user:secret@{}",
        fake.endpoint().trim_start_matches("http://")
    ));
    bad.push(config);
    for config in bad {
        let built = AzureStore::new(config.clone()).await;
        match built {
            Err(StoreError::Permanent(message)) => {
                assert!(!message.contains("secret"), "{message}");
            }
            other => panic!("{config:?}: {other:?}"),
        }
    }
    assert!(fake.requests().is_empty(), "{:?}", fake.requests());
}

#[test]
fn the_store_authority_is_the_endpoints_host_and_port() {
    assert_eq!(
        store_authority("acct", None).expect("default"),
        "acct.blob.core.windows.net"
    );
    assert_eq!(
        store_authority("acct", Some("http://127.0.0.1:10000/acct")).expect("emulator"),
        "127.0.0.1:10000"
    );
    assert!(store_authority("acct", Some("http:///acct")).is_err());
}

/// Every byte but ASCII letters and digits as `%XX`, as the fake and Azure
/// encode a `Name` element.
fn encode_all(name: &str) -> String {
    name.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// `text` with XML's five special characters escaped.
fn xml_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

proptest! {
    #[test]
    fn parse_xml_never_panics(body in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = parse_xml(&body);
    }

    #[test]
    fn parse_xml_never_panics_on_near_xml(body in "[<>/=\"a-zA-Z &;#x0-9!\\[\\]?-]{0,200}") {
        let _ = parse_xml(body.as_bytes());
    }

    #[test]
    fn parse_list_gives_a_page_or_a_typed_error(
        body in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        match parse_list("p/", &body) {
            Ok(_) | Err(StoreError::Permanent(_)) => {}
            Err(other) => prop_assert!(false, "{other:?}"),
        }
    }

    #[test]
    fn parse_list_gives_a_page_or_a_typed_error_on_near_list_xml(
        body in "<EnumerationResults>(<Blobs>(<Blob>(<Name( Encoded=\"true\")?>[%0-9A-Fa-z/]{0,12}</Name>)?(<Properties>(<Etag>[0-9xA-F\"]{0,6}</Etag>)?(<Content-Length>[0-9-]{0,4}</Content-Length>)?(<Last-Modified>[A-Za-z0-9 ,:]{0,30}</Last-Modified>)?</Properties>)?</Blob>|<BlobPrefix>(<Name>[a-z/]{0,6}</Name>)?</BlobPrefix>){0,3}</Blobs>)?(<NextMarker>[a-z0-9]{0,4}</NextMarker>)?</EnumerationResults>",
    ) {
        match parse_list("p/", body.as_bytes()) {
            Ok(_) | Err(StoreError::Permanent(_)) => {}
            Err(other) => prop_assert!(false, "{other:?}"),
        }
    }

    /// A well-formed response lists every name exactly as stored, encoded or
    /// not, with the HEAD-quoted ETag.
    #[test]
    fn parse_list_returns_every_listed_name(
        names in proptest::collection::vec(("\\PC{1,16}", any::<bool>()), 0..6),
        marker in proptest::option::of("[a-z0-9]{1,8}"),
    ) {
        let mut body = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults><Blobs>");
        for (name, encoded) in &names {
            body.push_str("<Blob>");
            if *encoded {
                body.push_str(&format!("<Name Encoded=\"true\">{}</Name>", encode_all(name)));
            } else {
                body.push_str(&format!("<Name>{}</Name>", xml_text(name)));
            }
            body.push_str(
                "<Properties><Last-Modified>Thu, 12 Nov 2026 21:50:51 GMT</Last-Modified>\
                 <Etag>0x8DE0000000000001</Etag><Content-Length>3</Content-Length>\
                 </Properties></Blob>",
            );
        }
        body.push_str("</Blobs>");
        match &marker {
            Some(marker) => body.push_str(&format!("<NextMarker>{marker}</NextMarker>")),
            None => body.push_str("<NextMarker />"),
        }
        body.push_str("</EnumerationResults>");

        let parsed = parse_list("", body.as_bytes()).expect("a well-formed listing");
        let listed: Vec<&str> = parsed.items.iter().map(|m| m.key.as_str()).collect();
        let expected: Vec<&str> = names.iter().map(|(name, _)| name.as_str()).collect();
        prop_assert_eq!(listed, expected);
        for item in &parsed.items {
            prop_assert_eq!(item.etag.0.as_str(), "\"0x8DE0000000000001\"");
            prop_assert_eq!(&item.version.0, &item.etag.0);
            prop_assert_eq!(item.size, 3);
        }
        prop_assert_eq!(parsed.next_marker, marker);
    }

    #[test]
    fn decode_encoded_name_never_panics(text in "[%0-9A-Fa-fg-z/\\x00-\\x7f\u{e9}\u{1f600}]{0,64}") {
        let _ = decode_encoded_name(&text);
    }

    #[test]
    fn an_encoded_name_round_trips(name in "\\PC{0,32}") {
        prop_assert_eq!(decode_encoded_name(&encode_all(&name)), name);
    }
}
