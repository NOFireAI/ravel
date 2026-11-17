//! The driver's actions and its model of what each one wrote.
//!
//! Every action works under its own fresh key prefix, so the driver knows
//! every write that could have touched those keys and can judge each read
//! against them alone. An action returns a [`Verdict`] for the property it
//! checks, or `Err` when faults left it without a definite read to judge
//! (an inconclusive action asserts nothing).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PageToken, PutMode, PutOptions, PutOutcome, StoreError, Version,
};
use serde_json::{Value, json};

use crate::mutant::BrokenStore;

/// A read is retried this often while the store answers with a retryable
/// error, then the action is inconclusive.
const SETTLE_TRIES: u32 = 40;
const SETTLE_DELAY: Duration = Duration::from_millis(250);
/// Retries of one listing page that failed with a retryable error.
const PAGE_TRIES: u32 = 8;

/// The judgement of one action.
#[derive(Debug, Default)]
pub struct Verdict {
    pub holds: bool,
    pub details: Value,
    /// A create race had one `Ok` winner and at least one `AlreadyExists`.
    pub contended: bool,
    /// A CAS with a stale version was answered `PreconditionFailed`.
    pub stale_refused: bool,
    /// A write failed with a retryable error.
    pub failed: bool,
    /// A write that failed was later found applied: a real lost response.
    pub failed_applied: bool,
}

pub type Judged = Result<Verdict, String>;

/// What one key holds, from a read the store answered definitely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Absent,
    Present { data: Bytes, version: String },
}

impl Read {
    fn holds(&self, want: &[u8]) -> bool {
        matches!(self, Read::Present { data, .. } if data.as_ref() == want)
    }

    fn describe(&self) -> Value {
        match self {
            Read::Absent => json!("absent"),
            Read::Present { data, version } => json!({
                "data": String::from_utf8_lossy(&data[..data.len().min(120)]),
                "len": data.len(),
                "version": version,
            }),
        }
    }
}

/// The caller's view of one put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Put {
    Ok(String),
    AlreadyExists,
    PreconditionFailed,
    Failed { retryable: bool, error: String },
}

impl Put {
    fn of(r: Result<PutOutcome, StoreError>) -> Put {
        match r {
            Ok(o) => Put::Ok(o.version.0),
            Err(StoreError::AlreadyExists) => Put::AlreadyExists,
            Err(StoreError::PreconditionFailed) => Put::PreconditionFailed,
            Err(e) => Put::Failed {
                retryable: e.is_retryable(),
                error: e.to_string(),
            },
        }
    }

    fn describe(&self) -> Value {
        json!(format!("{self:?}"))
    }
}

/// The bytes of one write: unique to the action and the write within it, so a
/// read names the write it returns.
pub fn body(base: &str, tag: &str) -> Bytes {
    Bytes::from(format!("{base}/{tag}"))
}

fn opts(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        checksum: None,
    }
}

fn cas_opts(version: &str) -> PutOptions {
    opts(PutMode::CasVersion(Version(version.to_string())))
}

/// Read `key` until the store answers definitely.
pub async fn read_settled(store: &dyn ObjectStoreBackend, key: &str) -> Result<Read, String> {
    let mut last = String::new();
    for _ in 0..SETTLE_TRIES {
        match store.get(key, GetRange::Full).await {
            Ok(o) => {
                return Ok(Read::Present {
                    data: o.data,
                    version: o.version.0,
                });
            }
            Err(StoreError::NotFound) => return Ok(Read::Absent),
            Err(e) if e.is_retryable() => {
                last = e.to_string();
                tokio::time::sleep(SETTLE_DELAY).await;
            }
            Err(e) => return Err(format!("read of {key}: {e}")),
        }
    }
    Err(format!("read of {key} never settled: {last}"))
}

/// CreateIfAbsentWinnerUnique: concurrent creates of one fresh key. At most
/// one is answered `Ok`; once the key is present it holds one contender's
/// bytes, the `Ok` one's when there is one; `AlreadyExists` implies present.
/// A contender answered `AlreadyExists` may still hold the key: a retried
/// create after a lost response reads its own earlier effect as a conflict,
/// which ravel-commit's writers take as success.
pub async fn create_race(store: &Arc<BrokenStore>, base: &str, contenders: usize) -> Judged {
    let key = format!("{base}/k");
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..contenders {
        let store = Arc::clone(store);
        let key = key.clone();
        let data = body(base, &format!("c{i}"));
        tasks.spawn(async move {
            let r = store
                .put(&key, data, PutOptions::create_if_absent())
                .await;
            (i, Put::of(r))
        });
    }
    let mut outcomes: Vec<Option<Put>> = vec![None; contenders];
    while let Some(joined) = tasks.join_next().await {
        let (i, put) = joined.map_err(|e| format!("create task: {e}"))?;
        if let Some(slot) = outcomes.get_mut(i) {
            *slot = Some(put);
        }
    }
    let outcomes: Vec<Put> = outcomes.into_iter().flatten().collect();
    let read = read_settled(store.as_ref(), &key).await?;
    Ok(judge_create_race(base, &outcomes, &read))
}

pub fn judge_create_race(base: &str, outcomes: &[Put], read: &Read) -> Verdict {
    let oks: Vec<usize> = (0..outcomes.len())
        .filter(|&i| matches!(outcomes[i], Put::Ok(_)))
        .collect();
    let exists = outcomes
        .iter()
        .filter(|p| matches!(p, Put::AlreadyExists))
        .count();
    let stored_by = match read {
        Read::Absent => None,
        Read::Present { data, .. } => {
            (0..outcomes.len()).find(|&i| data.as_ref() == body(base, &format!("c{i}")).as_ref())
        }
    };
    let holds = oks.len() <= 1
        && match read {
            Read::Absent => oks.is_empty() && exists == 0,
            Read::Present { version, .. } => match (stored_by, oks.first()) {
                (Some(w), Some(&ok)) => {
                    w == ok && outcomes.get(ok) == Some(&Put::Ok(version.clone()))
                }
                (Some(w), None) => !matches!(outcomes.get(w), Some(Put::PreconditionFailed)),
                (None, _) => false,
            },
        };
    Verdict {
        holds,
        contended: oks.len() == 1 && exists >= 1,
        details: json!({
            "base": base,
            "outcomes": outcomes.iter().map(Put::describe).collect::<Vec<_>>(),
            "read": read.describe(),
            "stored_by": stored_by,
        }),
        ..Verdict::default()
    }
}

/// CasOutcomeMatchesEffect: a CAS answered `Ok` applied its bytes under the
/// version it returned; one answered otherwise applied nothing; a CAS with a
/// stale version, or on an absent key, never applies.
pub async fn cas(store: &BrokenStore, base: &str) -> Judged {
    let key = format!("{base}/k");
    let (a, b, c, d) = (
        body(base, "a"),
        body(base, "b"),
        body(base, "c"),
        body(base, "d"),
    );
    let va = match store.put(&key, a.clone(), PutOptions::default()).await {
        Ok(o) => o.version.0,
        Err(e) => return Err(format!("setup put: {e}")),
    };
    let fresh = Put::of(store.put(&key, b.clone(), cas_opts(&va)).await);
    let after_fresh = read_settled(store, &key).await?;
    let fresh_ok = match &fresh {
        Put::Ok(v) => {
            matches!(&after_fresh, Read::Present { data, version } if *data == b && version == v)
        }
        Put::Failed { .. } => after_fresh.holds(&a) || after_fresh.holds(&b),
        _ => matches!(&after_fresh, Read::Present { data, version } if *data == a && *version == va),
    };

    // The stale CAS needs the key to have moved past `va`.
    let mut stale: Option<(Put, Read)> = None;
    if after_fresh.holds(&b) {
        let put = Put::of(store.put(&key, c.clone(), cas_opts(&va)).await);
        let read = read_settled(store, &key).await?;
        stale = Some((put, read));
    }
    let stale_ok = match &stale {
        None => true,
        Some((put, read)) => {
            !matches!(put, Put::Ok(_)) && !read.holds(&c) && (put != &Put::PreconditionFailed || read.holds(&b))
        }
    };

    let absent_key = format!("{base}/absent");
    let absent_put = Put::of(store.put(&absent_key, d, cas_opts(&va)).await);
    let absent_read = read_settled(store, &absent_key).await?;
    let absent_ok = !matches!(absent_put, Put::Ok(_)) && absent_read == Read::Absent;

    Ok(Verdict {
        holds: fresh_ok && stale_ok && absent_ok,
        stale_refused: matches!(&stale, Some((Put::PreconditionFailed, _))),
        details: json!({
            "base": base,
            "fresh": { "put": fresh.describe(), "read": after_fresh.describe(), "ok": fresh_ok },
            "stale": stale.as_ref().map(|(p, r)| json!({ "put": p.describe(), "read": r.describe() })),
            "stale_ok": stale_ok,
            "absent": { "put": absent_put.describe(), "read": absent_read.describe(), "ok": absent_ok },
        }),
        ..Verdict::default()
    })
}

/// ReadAfterWrite: after each successful overwrite, a read returns its bytes
/// and the version the put returned.
pub async fn read_after_write(store: &BrokenStore, base: &str, writes: usize) -> Judged {
    let key = format!("{base}/k");
    let mut checked = Vec::new();
    let mut holds = true;
    for i in 0..writes {
        let data = body(base, &format!("w{i}"));
        let put = Put::of(store.put(&key, data.clone(), PutOptions::default()).await);
        let Put::Ok(version) = &put else {
            // A failed overwrite leaves the key's value unknown to the model.
            break;
        };
        let read = read_settled(store, &key).await?;
        let ok = matches!(&read, Read::Present { data: d, version: v } if *d == data && v == version);
        holds &= ok;
        checked.push(json!({ "put": put.describe(), "read": read.describe() }));
    }
    if checked.is_empty() {
        return Err("no overwrite succeeded".to_string());
    }
    Ok(Verdict {
        holds,
        details: json!({ "base": base, "checked": checked }),
        ..Verdict::default()
    })
}

fn delete_ok(r: &Result<(), StoreError>) -> bool {
    match r {
        Ok(()) => true,
        Err(e) => e.is_retryable(),
    }
}

/// DeleteIdempotent: deleting a key, deleting it again, and deleting a key
/// that never existed each succeed (or fail retryably), leave the deleted
/// keys absent and leave a sibling untouched.
pub async fn delete_idempotent(store: &BrokenStore, base: &str) -> Judged {
    let (gone, sibling, never) = (
        format!("{base}/gone"),
        format!("{base}/sibling"),
        format!("{base}/never"),
    );
    let sibling_body = body(base, "sibling");
    for (key, data) in [(&gone, body(base, "gone")), (&sibling, sibling_body.clone())] {
        if let Err(e) = store.put(key, data, PutOptions::default()).await {
            return Err(format!("setup put of {key}: {e}"));
        }
    }
    let first = store.delete(&gone).await;
    let second = store.delete(&gone).await;
    let absent = store.delete(&never).await;
    let answers_ok = delete_ok(&first) && delete_ok(&second) && delete_ok(&absent);
    let deleted = first.is_ok() || second.is_ok();
    let gone_read = read_settled(store, &gone).await?;
    let never_read = read_settled(store, &never).await?;
    let sibling_read = read_settled(store, &sibling).await?;
    let reads_ok = (!deleted || gone_read == Read::Absent)
        && never_read == Read::Absent
        && sibling_read.holds(&sibling_body);
    // The listing is evidence only when it completes; a failed listing is
    // another property's business.
    let listed = match ravel_object_store::list_all(store, &format!("{base}/")).await {
        Ok(objects) => Some(objects.into_iter().map(|m| m.key).collect::<Vec<_>>()),
        Err(_) => None,
    };
    let list_ok = match &listed {
        Some(keys) => !deleted || keys == std::slice::from_ref(&sibling),
        None => true,
    };
    Ok(Verdict {
        holds: answers_ok && reads_ok && list_ok,
        details: json!({
            "base": base,
            "deletes": [format!("{first:?}"), format!("{second:?}"), format!("{absent:?}")],
            "gone": gone_read.describe(),
            "never": never_read.describe(),
            "sibling": sibling_read.describe(),
            "listed": listed,
        }),
        ..Verdict::default()
    })
}

/// VersionsNeverReused: create, delete and create again mints a version
/// distinct from the first, and a CAS holding the pre-delete version fails
/// and applies nothing.
pub async fn versions_never_reused(store: &BrokenStore, base: &str) -> Judged {
    let key = format!("{base}/k");
    let (a, b, c) = (body(base, "a"), body(base, "b"), body(base, "c"));
    let first = match store.put(&key, a, PutOptions::create_if_absent()).await {
        Ok(o) => o.version.0,
        Err(e) => return Err(format!("first create: {e}")),
    };
    store
        .delete(&key)
        .await
        .map_err(|e| format!("delete: {e}"))?;
    let second = match store.put(&key, b.clone(), PutOptions::create_if_absent()).await {
        Ok(o) => o.version.0,
        Err(e) => return Err(format!("second create: {e}")),
    };
    let put = Put::of(store.put(&key, c.clone(), cas_opts(&first)).await);
    let read = read_settled(store, &key).await?;
    let holds = second != first && !matches!(put, Put::Ok(_)) && read.holds(&b);
    Ok(Verdict {
        holds,
        details: json!({
            "base": base,
            "first": first,
            "second": second,
            "stale_cas": put.describe(),
            "read": read.describe(),
        }),
        ..Verdict::default()
    })
}

/// Whether `key` is invisible: absent to a read and to a listing of `prefix`.
async fn invisible(store: &BrokenStore, prefix: &str, key: &str) -> Result<bool, String> {
    let read = read_settled(store, key).await?;
    let listed = match ravel_object_store::list_all(store, prefix).await {
        Ok(objects) => objects.iter().any(|m| m.key == key),
        Err(_) => false,
    };
    Ok(read == Read::Absent && !listed)
}

/// MultipartInvisibleUntilComplete: nothing an upload staged is visible
/// before `complete`; after `complete` the key holds every part in order,
/// and after `abort` it holds nothing.
pub async fn multipart(store: &BrokenStore, base: &str, parts: &[Bytes], complete: bool) -> Judged {
    let key = format!("{base}/k");
    let prefix = format!("{base}/");
    let mut upload = store
        .put_multipart(&key)
        .await
        .map_err(|e| format!("begin: {e}"))?;
    let mut before = Vec::new();
    let mut staged = 0;
    for part in parts {
        if let Err(e) = upload.put_part(part.clone(), None).await {
            before.push(json!(format!("part {staged} failed: {e}")));
            break;
        }
        staged += 1;
        before.push(json!(invisible(store, &prefix, &key).await?));
    }
    let invisible_before = before.iter().all(|v| v.as_bool() != Some(false));
    let whole: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
    let (end, read) = if complete && staged == parts.len() {
        let r = upload.complete().await;
        let read = read_settled(store, &key).await?;
        let ok = match &r {
            Ok(_) => read.holds(&whole),
            Err(_) => read == Read::Absent || read.holds(&whole),
        };
        (json!({ "complete": format!("{:?}", r.map(|o| o.version.0)), "ok": ok }), read)
    } else {
        let r = upload.abort().await;
        let read = read_settled(store, &key).await?;
        let ok = read == Read::Absent;
        (json!({ "abort": format!("{r:?}"), "ok": ok }), read)
    };
    Ok(Verdict {
        holds: invisible_before && end["ok"].as_bool() == Some(true),
        details: json!({
            "base": base,
            "parts": parts.iter().map(Bytes::len).collect::<Vec<_>>(),
            "invisible_after_each_part": before,
            "end": end,
            "read": read.describe(),
        }),
        ..Verdict::default()
    })
}

/// ListEventuallyComplete: a paged listing of a prefix whose keys were all
/// written before it began ends, within one page per `page_size` keys plus
/// one, holding every key written and nothing else.
pub async fn list_paged(store: &BrokenStore, base: &str, keys: usize, page_size: usize) -> Judged {
    let prefix = format!("{base}/");
    let mut written = BTreeSet::new();
    let mut possible = BTreeSet::new();
    for i in 0..keys {
        let key = format!("{prefix}{i:03}");
        possible.insert(key.clone());
        if store
            .put(&key, body(base, &i.to_string()), PutOptions::default())
            .await
            .is_ok()
        {
            written.insert(key);
        }
    }
    let bound = possible.len().div_ceil(page_size.max(1)) + 1;
    let mut seen = BTreeSet::new();
    let mut token: Option<PageToken> = None;
    let mut pages = 0;
    let mut ended = false;
    while pages < bound {
        let mut tries = 0;
        let page = loop {
            match store.list(&prefix, token.clone()).await {
                Ok(page) => break page,
                Err(e) if e.is_retryable() && tries + 1 < PAGE_TRIES => {
                    tries += 1;
                    tokio::time::sleep(SETTLE_DELAY).await;
                }
                Err(e) => return Err(format!("page {pages}: {e}")),
            }
        };
        pages += 1;
        seen.extend(page.objects.into_iter().map(|m| m.key));
        match page.next {
            Some(next) => token = Some(next),
            None => {
                ended = true;
                break;
            }
        }
    }
    let missing: Vec<&String> = written.difference(&seen).collect();
    let foreign: Vec<&String> = seen.difference(&possible).collect();
    Ok(Verdict {
        holds: ended && missing.is_empty() && foreign.is_empty(),
        details: json!({
            "base": base,
            "keys": keys,
            "written": written.len(),
            "page_size": page_size,
            "bound": bound,
            "pages": pages,
            "ended": ended,
            "missing": missing,
            "foreign": foreign,
        }),
        ..Verdict::default()
    })
}

/// LostResponseEffectApplied: an overwrite the store acknowledged, whose
/// response the caller lost, is applied: the key holds its bytes.
pub async fn lost_response(store: &BrokenStore, base: &str) -> Judged {
    let key = format!("{base}/k");
    let data = body(base, "lost");
    store
        .put_dropping_response(&key, data.clone())
        .await
        .map_err(|e| format!("write failed before its acknowledgement: {e}"))?;
    let read = read_settled(store, &key).await?;
    Ok(Verdict {
        holds: read.holds(&data),
        details: json!({ "base": base, "read": read.describe() }),
        ..Verdict::default()
    })
}

/// TransientLeavesNothing: a create that failed retryably left either
/// nothing or the whole write, never a partial or foreign object. `hurried`
/// is a store whose short request timeout makes some writes fail; `store`
/// reads with the normal timeout.
pub async fn transient(
    hurried: &BrokenStore,
    store: &BrokenStore,
    base: &str,
    len: usize,
) -> Judged {
    let key = format!("{base}/k");
    let mut data = body(base, "t").to_vec();
    data.resize(len.max(data.len()), b'.');
    let put = Put::of(
        hurried
            .put(&key, Bytes::from(data.clone()), PutOptions::create_if_absent())
            .await,
    );
    let failed = matches!(put, Put::Failed { retryable: true, .. });
    if !failed {
        return Ok(Verdict {
            holds: true,
            details: json!({ "base": base, "put": put.describe() }),
            ..Verdict::default()
        });
    }
    let read = read_settled(store, &key).await?;
    Ok(Verdict {
        holds: read == Read::Absent || read.holds(&data),
        failed,
        failed_applied: read.holds(&data),
        details: json!({ "base": base, "put": put.describe(), "read": read.describe() }),
        ..Verdict::default()
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::mutant::Mutant;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime")
    }

    fn memory(page_size: usize, mutant: Option<Mutant>) -> Arc<BrokenStore> {
        Arc::new(BrokenStore::new(
            Arc::new(MemoryStore::with_page_size(page_size)),
            mutant,
        ))
    }

    fn failing_first_put(inner: MemoryStore) -> FaultStore<MemoryStore> {
        let rule = Rule::new(Op::Put, ScriptedFault::Transient("injected".to_string()))
            .with_occurrence(Occurrence::Nth(1));
        FaultStore::new(inner, FaultPlan::empty().with_rule(rule))
    }

    fn holds(v: Judged) -> bool {
        v.expect("conclusive").holds
    }

    #[test]
    fn every_property_holds_against_memory_store() {
        let rt = rt();
        let s = memory(2, None);
        rt.block_on(async {
            for n in 1..=4 {
                let v = create_race(&s, &format!("race{n}"), n).await.expect("race");
                assert!(v.holds, "create race {n}: {}", v.details);
                assert_eq!(v.contended, n > 1);
            }
            let v = cas(&s, "cas").await.expect("cas");
            assert!(v.holds, "{}", v.details);
            assert!(v.stale_refused);
            assert!(holds(read_after_write(&s, "raw", 3).await));
            assert!(holds(delete_idempotent(&s, "del").await));
            assert!(holds(versions_never_reused(&s, "ver").await));
            let small = vec![Bytes::from_static(b"only part")];
            assert!(holds(multipart(&s, "mp1", &small, true).await));
            assert!(holds(multipart(&s, "mp2", &small, false).await));
            let two = vec![
                Bytes::from(vec![b'x'; ravel_object_store::MULTIPART_MIN_PART_SIZE]),
                Bytes::from_static(b"tail"),
            ];
            assert!(holds(multipart(&s, "mp3", &two, true).await));
            for keys in [1, 2, 5, 9] {
                let v = list_paged(&s, &format!("list{keys}"), keys, 2).await.expect("list");
                assert!(v.holds, "{}", v.details);
            }
            assert!(holds(lost_response(&s, "lost").await));
            let v = transient(&s, &s, "tr", 64).await.expect("transient");
            assert!(v.holds && !v.failed);
        });
    }

    #[test]
    fn broken_cas_store_is_caught() {
        let rt = rt();
        let s = memory(2, Some(inv_mutant!("common/cas-accepts-stale-version")));
        let v = rt.block_on(cas(&s, "cas")).expect("cas");
        assert!(!v.holds, "the CAS property held under the mutant: {}", v.details);
        assert_eq!(v.details["stale_ok"], json!(false));
        assert!(!v.stale_refused);
    }

    #[test]
    fn dropped_lost_response_is_caught() {
        let rt = rt();
        let s = memory(2, Some(inv_mutant!("common/lost-response-not-applied")));
        let v = rt.block_on(lost_response(&s, "lost")).expect("lost");
        assert!(!v.holds, "{}", v.details);
        assert_eq!(v.details["read"], json!("absent"));
    }

    #[test]
    fn stalled_listing_is_caught() {
        let rt = rt();
        let s = memory(2, Some(inv_mutant!("common/list-never-progresses")));
        let v = rt.block_on(list_paged(&s, "list", 5, 2)).expect("list");
        assert!(!v.holds, "{}", v.details);
        assert_eq!(v.details["ended"], json!(false));
        assert_eq!(v.details["pages"], json!(4));
    }

    #[test]
    fn each_mutant_breaks_only_its_own_property() {
        let rt = rt();
        for m in Mutant::ALL {
            let s = memory(2, Some(m));
            let (cas_holds, lost_holds, list_holds) = rt.block_on(async {
                (
                    holds(cas(&s, "cas").await),
                    holds(lost_response(&s, "lost").await),
                    holds(list_paged(&s, "list", 5, 2).await),
                )
            });
            assert_eq!(cas_holds, m != Mutant::CasAcceptsStaleVersion, "{m:?}");
            assert_eq!(lost_holds, m != Mutant::LostResponseNotApplied, "{m:?}");
            assert_eq!(list_holds, m != Mutant::ListNeverProgresses, "{m:?}");
        }
    }

    #[test]
    fn a_transient_put_failure_is_judged() {
        let rt = rt();
        let faults = Arc::new(failing_first_put(MemoryStore::new()));
        let hurried = BrokenStore::new(faults.clone(), None);
        let s = memory(2, None);
        let v = rt
            .block_on(transient(&hurried, &s, "tr", 64))
            .expect("transient");
        assert!(v.failed && v.holds && !v.failed_applied, "{}", v.details);
        assert_eq!(faults.fault_count(Op::Put, FaultKind::Transient), 1);
    }

    #[test]
    fn a_partial_object_after_a_failed_put_is_caught() {
        let rt = rt();
        let inner = Arc::new(MemoryStore::new());
        let faults = Arc::new(failing_first_put(MemoryStore::new()));
        let hurried = BrokenStore::new(faults, None);
        let reader = BrokenStore::new(inner.clone(), None);
        rt.block_on(async {
            inner
                .put("tr/k", Bytes::from_static(b"tr/t trunc"), PutOptions::default())
                .await
                .expect("plant");
            let v = transient(&hurried, &reader, "tr", 64).await.expect("transient");
            assert!(v.failed && !v.holds, "{}", v.details);
        });
    }

    #[test]
    fn create_race_judgement_refuses_two_winners_and_a_foreign_value() {
        let ok = |v: &str| Put::Ok(v.to_string());
        let present = |tag: &str, v: &str| Read::Present {
            data: body("b", tag),
            version: v.to_string(),
        };
        assert!(judge_create_race("b", &[ok("1"), Put::AlreadyExists], &present("c0", "1")).holds);
        assert!(!judge_create_race("b", &[ok("1"), ok("2")], &present("c0", "1")).holds);
        assert!(!judge_create_race("b", &[ok("1"), Put::AlreadyExists], &present("c1", "1")).holds);
        assert!(!judge_create_race("b", &[ok("1")], &present("zz", "1")).holds);
        assert!(!judge_create_race("b", &[Put::AlreadyExists], &Read::Absent).holds);
        assert!(!judge_create_race("b", &[ok("1")], &Read::Absent).holds);
    }

    #[test]
    fn an_unknown_mutant_name_is_refused() {
        assert_eq!(Mutant::select(None), Ok(None));
        assert_eq!(Mutant::select(Some("")), Ok(None));
        assert_eq!(
            Mutant::select(Some("common/cas-accepts-stale-version")),
            Ok(Some(Mutant::CasAcceptsStaleVersion))
        );
        assert!(Mutant::select(Some("common/no-such-mutant")).is_err());
        assert!(Mutant::select(Some("commit/commit-before-data")).is_err());
    }
}
