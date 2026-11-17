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
    /// The same-bytes recreate of [`versions_never_reused`], `None` when a
    /// fault left it inconclusive.
    pub same_bytes: Option<SameBytes>,
}

/// The same-bytes recreate sub-check of [`versions_never_reused`].
#[derive(Debug)]
pub struct SameBytes {
    /// A CAS holding the pre-delete version matched its effect after the key
    /// was deleted and recreated with identical bytes.
    pub ok: bool,
    /// The recreate minted the version it had before the delete: `S3Store`'s
    /// content-derived ETag reused. Recorded, not required.
    pub reused: bool,
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
            let r = store.put(&key, data, PutOptions::create_if_absent()).await;
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
        // object_store retries a CAS on 5xx and 409. A CAS that applied B and
        // was then retried is answered PreconditionFailed though its write
        // landed (the retry conflicts with B's own fresh version), like
        // create_race accepting an AlreadyExists from its own earlier write.
        // Accept that only when a re-read shows exactly this CAS's own write:
        // bytes B are unique to it, so B present means this CAS applied. Else
        // the key must still hold A under A's version.
        Put::PreconditionFailed => {
            after_fresh.holds(&b)
                || matches!(&after_fresh, Read::Present { data, version } if *data == a && *version == va)
        }
        _ => {
            matches!(&after_fresh, Read::Present { data, version } if *data == a && *version == va)
        }
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
            !matches!(put, Put::Ok(_))
                && !read.holds(&c)
                && (put != &Put::PreconditionFailed || read.holds(&b))
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
        let ok =
            matches!(&read, Read::Present { data: d, version: v } if *d == data && v == version);
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
    for (key, data) in [
        (&gone, body(base, "gone")),
        (&sibling, sibling_body.clone()),
    ] {
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

/// VersionsNeverReused: create, delete and create again with different bytes
/// mints a version distinct from the first, and a CAS holding the pre-delete
/// version fails and applies nothing.
///
/// The same-bytes recreate sub-check exercises the case the different-bytes
/// body hides: `S3Store`'s version is the object's ETag, derived from its
/// content, so a delete-and-recreate with identical bytes gets the same
/// version back. Under that reuse a CAS holding the pre-delete version targets
/// the current version and may legitimately apply; the contract the driver can
/// hold against a real store is only that the CAS outcome matches its effect
/// (`Ok` means the version was reused and the key holds the CAS bytes; any
/// other answer means the key holds the recreate, or, under reuse, the CAS
/// bytes of a CAS that applied and lost its response). A fault in the
/// same-bytes setup leaves only the sub-check inconclusive. Issue #4389 tracks
/// whether Ravel's own CAS targets can collide a version this way.
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
    let second = match store
        .put(&key, b.clone(), PutOptions::create_if_absent())
        .await
    {
        Ok(o) => o.version.0,
        Err(e) => return Err(format!("second create: {e}")),
    };
    let put = Put::of(store.put(&key, c.clone(), cas_opts(&first)).await);
    let read = read_settled(store, &key).await?;
    let holds = second != first && !matches!(put, Put::Ok(_)) && read.holds(&b);

    // The different-bytes verdict stands on its own: a fault in the same-bytes
    // setup leaves only the sub-check inconclusive.
    let (same_bytes, same_details) = match same_bytes_recreate(store, base).await {
        Ok((sb, d)) => (Some(sb), d),
        Err(e) => (None, json!({ "inconclusive": e })),
    };
    Ok(Verdict {
        holds,
        same_bytes,
        details: json!({
            "base": base,
            "first": first,
            "second": second,
            "stale_cas": put.describe(),
            "read": read.describe(),
            "same_bytes": same_details,
        }),
        ..Verdict::default()
    })
}

/// Create X, delete, create X again, then a CAS of Y holding the first
/// version V1.
async fn same_bytes_recreate(
    store: &BrokenStore,
    base: &str,
) -> Result<(SameBytes, Value), String> {
    let key = format!("{base}/same");
    let (x, y) = (body(base, "x"), body(base, "y"));
    let v1 = match store
        .put(&key, x.clone(), PutOptions::create_if_absent())
        .await
    {
        Ok(o) => o.version.0,
        Err(e) => return Err(format!("same-bytes first create: {e}")),
    };
    store
        .delete(&key)
        .await
        .map_err(|e| format!("same-bytes delete: {e}"))?;
    let v2 = match store
        .put(&key, x.clone(), PutOptions::create_if_absent())
        .await
    {
        Ok(o) => o.version.0,
        Err(e) => return Err(format!("same-bytes recreate: {e}")),
    };
    let cas = Put::of(store.put(&key, y.clone(), cas_opts(&v1)).await);
    let read = read_settled(store, &key).await?;
    let reused = v2 == v1;
    // With V2 == V1 the CAS held the current version, so it may apply, and one
    // that applied but whose response was lost or retried is answered Failed
    // or PreconditionFailed: Y in place is conformant whatever the answer,
    // since Y is unique to this CAS. With V2 != V1 the CAS held a stale version
    // and must leave X in place. A CAS answered Ok must hold Y.
    let ok = match &cas {
        Put::Ok(_) => reused && read.holds(&y),
        _ => read.holds(&x) || (reused && read.holds(&y)),
    };
    let details = json!({
        "v1": v1,
        "v2": v2,
        "reused": reused,
        "cas": cas.describe(),
        "read": read.describe(),
        "ok": ok,
    });
    Ok((SameBytes { ok, reused }, details))
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
        (
            json!({ "complete": format!("{:?}", r.map(|o| o.version.0)), "ok": ok }),
            read,
        )
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
            .put(
                &key,
                Bytes::from(data.clone()),
                PutOptions::create_if_absent(),
            )
            .await,
    );
    let failed = matches!(
        put,
        Put::Failed {
            retryable: true,
            ..
        }
    );
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, ListPage, MultipartUpload, ObjectMeta,
        UploadChecksum,
    };

    use super::*;
    use crate::mutant::Mutant;

    /// A content-derived version, like `S3Store`'s ETag: equal bytes get equal
    /// versions back.
    fn content_version(data: &[u8]) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        data.hash(&mut h);
        format!("cv-{:016x}", h.finish())
    }

    /// A `MemoryStore` with one quirk, to model a real backend's edge:
    /// - `AppliesThenRefusesFirstCas`: the first `CasVersion` put applies its
    ///   bytes and is then answered `PreconditionFailed`, as object_store's
    ///   retry of a CAS that already landed (a 5xx/409 retry) is.
    /// - `ContentVersion`: every put reports a content-derived version, so a
    ///   same-bytes recreate reuses the version an ETag backend would.
    /// - `SameCasLandsThenAnswered`: a CAS on the same-bytes key applies as
    ///   the store would (against content versions when `reuse`, else as an
    ///   overwrite) and is then answered `answer()`, as a CAS whose response
    ///   was lost or retried is. Every other put is stored as `reuse` selects.
    /// - `MintsUnreadableVersion`: every put stores its bytes but reports a
    ///   version a following get never returns, modelling a store that violates
    ///   read-after-write on the version alone (the bytes round-trip).
    /// - `LeaksMultipartPart`: `put_multipart` returns an upload that publishes
    ///   each staged part under the key before `complete`, modelling a store
    ///   that violates multipart's invisible-until-complete contract. `put` is
    ///   unquirked.
    enum PutQuirk {
        AppliesThenRefusesFirstCas(AtomicUsize),
        ContentVersion,
        SameCasLandsThenAnswered {
            reuse: bool,
            answer: fn() -> StoreError,
        },
        MintsUnreadableVersion,
        LeaksMultipartPart,
    }

    struct QuirkStore {
        inner: Arc<MemoryStore>,
        quirk: PutQuirk,
    }

    impl QuirkStore {
        fn wrapped(quirk: PutQuirk) -> Arc<BrokenStore> {
            Arc::new(BrokenStore::new(
                Arc::new(QuirkStore {
                    inner: Arc::new(MemoryStore::new()),
                    quirk,
                }),
                None,
            ))
        }

        /// A put that reports a content-derived version and matches a held
        /// version against the content version of what is stored, as an ETag
        /// backend does, so a CAS against a reused version applies.
        async fn content_put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            let version = content_version(&data);
            let mode = match &opts.mode {
                PutMode::CasVersion(Version(want)) => {
                    match self.inner.get(key, GetRange::Full).await {
                        Ok(o) if content_version(&o.data) == *want => PutMode::Overwrite,
                        Ok(_) | Err(StoreError::NotFound) => {
                            return Err(StoreError::PreconditionFailed);
                        }
                        Err(e) => return Err(e),
                    }
                }
                other => other.clone(),
            };
            let opts = PutOptions {
                mode,
                checksum: opts.checksum,
            };
            let out = self.inner.put(key, data, opts).await?;
            Ok(PutOutcome {
                version: Version(version),
                ..out
            })
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for QuirkStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            match &self.quirk {
                PutQuirk::AppliesThenRefusesFirstCas(seen) => {
                    if matches!(opts.mode, PutMode::CasVersion(_))
                        && seen.fetch_add(1, Ordering::SeqCst) == 0
                    {
                        let overwrite = PutOptions {
                            mode: PutMode::Overwrite,
                            checksum: opts.checksum,
                        };
                        self.inner.put(key, data, overwrite).await?;
                        return Err(StoreError::PreconditionFailed);
                    }
                    self.inner.put(key, data, opts).await
                }
                PutQuirk::ContentVersion => self.content_put(key, data, opts).await,
                PutQuirk::SameCasLandsThenAnswered { reuse, answer } => {
                    let same_cas =
                        matches!(opts.mode, PutMode::CasVersion(_)) && key.ends_with("/same");
                    let applied = if *reuse {
                        self.content_put(key, data, opts).await
                    } else if same_cas {
                        let overwrite = PutOptions {
                            mode: PutMode::Overwrite,
                            checksum: opts.checksum,
                        };
                        self.inner.put(key, data, overwrite).await
                    } else {
                        self.inner.put(key, data, opts).await
                    };
                    match applied {
                        Ok(_) if same_cas => Err(answer()),
                        other => other,
                    }
                }
                PutQuirk::MintsUnreadableVersion => {
                    let out = self.inner.put(key, data, opts).await?;
                    Ok(PutOutcome {
                        version: Version(format!("phantom-{}", out.version.0)),
                        ..out
                    })
                }
                PutQuirk::LeaksMultipartPart => self.inner.put(key, data, opts).await,
            }
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            let inner = self.inner.put_multipart(key).await?;
            match &self.quirk {
                PutQuirk::LeaksMultipartPart => Ok(Box::new(LeakyUpload {
                    inner,
                    store: self.inner.as_ref(),
                    key: key.to_string(),
                })),
                _ => Ok(inner),
            }
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// A multipart upload that publishes each part under the key as it is
    /// staged, before `complete`, so a staged part is visible to a read and a
    /// listing. `complete` and `abort` still delegate to the real upload, so
    /// the only contract it breaks is invisible-until-complete.
    struct LeakyUpload<'a> {
        inner: Box<dyn MultipartUpload + 'a>,
        store: &'a MemoryStore,
        key: String,
    }

    #[async_trait]
    impl MultipartUpload for LeakyUpload<'_> {
        async fn put_part(
            &mut self,
            data: Bytes,
            checksum: Option<UploadChecksum>,
        ) -> Result<(), StoreError> {
            self.inner.put_part(data.clone(), checksum).await?;
            self.store
                .put(&self.key, data, PutOptions::default())
                .await?;
            Ok(())
        }

        async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> Result<(), StoreError> {
            self.inner.abort().await
        }
    }

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
            let v = versions_never_reused(&s, "ver").await.expect("ver");
            // MemoryStore mints a fresh version on recreate (a global counter,
            // not a content ETag), so the same-bytes recreate does not reuse
            // the version and its CAS sub-check holds.
            assert!(
                v.holds && v.same_bytes.as_ref().is_some_and(|sb| sb.ok && !sb.reused),
                "{}",
                v.details
            );
            let small = vec![Bytes::from_static(b"only part")];
            assert!(holds(multipart(&s, "mp1", &small, true).await));
            assert!(holds(multipart(&s, "mp2", &small, false).await));
            let two = vec![
                Bytes::from(vec![b'x'; ravel_object_store::MULTIPART_MIN_PART_SIZE]),
                Bytes::from_static(b"tail"),
            ];
            assert!(holds(multipart(&s, "mp3", &two, true).await));
            for keys in [1, 2, 5, 9] {
                let v = list_paged(&s, &format!("list{keys}"), keys, 2)
                    .await
                    .expect("list");
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
        assert!(
            !v.holds,
            "the CAS property held under the mutant: {}",
            v.details
        );
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
    fn a_version_a_read_does_not_return_is_caught() {
        let rt = rt();
        // The store stores each put's bytes but reports a version the following
        // get never returns. The bytes round-trip, so read_after_write reds on
        // the version comparison alone: dropping that comparison from its `ok`
        // turns this case green, so this test fails against that inversion.
        let s = QuirkStore::wrapped(PutQuirk::MintsUnreadableVersion);
        let v = rt
            .block_on(read_after_write(&s, "raw", 3))
            .expect("read_after_write conclusive");
        assert!(
            !v.holds,
            "read-after-write held though put's version was never read back: {}",
            v.details
        );
        // The bytes did round-trip: only the version differs, so the red is the
        // version comparison and nothing else.
        assert_eq!(v.details["checked"][0]["read"]["data"], json!("raw/w0"));
    }

    #[test]
    fn a_multipart_part_visible_before_complete_is_caught() {
        let rt = rt();
        // The upload publishes its one staged part under the key before
        // complete, so the staged part is visible to a read and a listing.
        // complete still assembles the whole object, so `end` is ok and the red
        // is invisible_before alone: dropping invisible_before from `holds`
        // turns this case green, so this test fails against that inversion.
        let s = QuirkStore::wrapped(PutQuirk::LeaksMultipartPart);
        let small = vec![Bytes::from_static(b"leaked part")];
        let v = rt
            .block_on(multipart(&s, "mp", &small, true))
            .expect("multipart conclusive");
        assert!(
            !v.holds,
            "multipart held though a staged part was visible before complete: {}",
            v.details
        );
        assert_eq!(v.details["invisible_after_each_part"], json!([false]));
        // The completed object is still whole, so only invisible_before reds.
        assert_eq!(v.details["end"]["ok"], json!(true));
    }

    #[test]
    fn each_mutant_breaks_only_its_own_property() {
        let rt = rt();
        for m in Mutant::ALL {
            let s = memory(2, Some(m));
            let two = vec![
                Bytes::from(vec![b'x'; ravel_object_store::MULTIPART_MIN_PART_SIZE]),
                Bytes::from_static(b"tail"),
            ];
            let (race, cas_h, raw, del, ver, mp, list, lost) = rt.block_on(async {
                (
                    holds(create_race(&s, "race", 3).await),
                    holds(cas(&s, "cas").await),
                    holds(read_after_write(&s, "raw", 3).await),
                    holds(delete_idempotent(&s, "del").await),
                    holds(versions_never_reused(&s, "ver").await),
                    holds(multipart(&s, "mp", &two, true).await),
                    holds(list_paged(&s, "list", 5, 2).await),
                    holds(lost_response(&s, "lost").await),
                )
            });
            // transient reads through the same store it writes through, which
            // carries no fault here, so no write fails and the property holds
            // under every mutant.
            let tr = rt.block_on(transient(&s, &s, "tr", 64)).expect("transient");

            // The CAS mutant breaks the CAS property and, as a side effect, the
            // versions property: both drive a stale CAS that it accepts as an
            // overwrite (README "Mutants").
            assert_eq!(
                cas_h,
                m != Mutant::CasAcceptsStaleVersion,
                "cas under {m:?}"
            );
            assert_eq!(
                ver,
                m != Mutant::CasAcceptsStaleVersion,
                "versions under {m:?}"
            );
            assert_eq!(
                lost,
                m != Mutant::LostResponseNotApplied,
                "lost under {m:?}"
            );
            assert_eq!(list, m != Mutant::ListNeverProgresses, "list under {m:?}");
            // The other five properties no mutant touches.
            assert!(race, "create race under {m:?}");
            assert!(raw, "read-after-write under {m:?}");
            assert!(del, "delete under {m:?}");
            assert!(mp, "multipart under {m:?}");
            assert!(
                tr.holds && !tr.failed,
                "transient under {m:?}: {}",
                tr.details
            );
        }
    }

    #[test]
    fn versions_same_bytes_recreate_sub_check() {
        let rt = rt();
        // A content-derived version (S3Store's ETag) reuses the version across
        // a delete and same-bytes recreate; the driver observes the reuse and
        // does not red: a CAS holding the pre-delete version is answered
        // against the current (equal) version, and the sub-check matches the
        // outcome to the effect.
        let cv = QuirkStore::wrapped(PutQuirk::ContentVersion);
        let v = rt.block_on(versions_never_reused(&cv, "ver")).expect("ver");
        let sb = v.same_bytes.expect("same-bytes conclusive");
        assert!(sb.reused, "reuse not observed: {}", v.details);
        assert!(sb.ok, "false red on reuse: {}", v.details);
        assert_eq!(v.details["same_bytes"]["reused"], json!(true));
        let fresh = memory(2, None);
        let v = rt
            .block_on(versions_never_reused(&fresh, "ver"))
            .expect("ver");
        assert_eq!(v.details["same_bytes"]["reused"], json!(false));

        // Accepting a stale CAS (common/cas-accepts-stale-version) breaks the
        // sub-check when V2 != V1: the CAS applies though the recreate minted a
        // fresh version.
        let m = memory(2, Some(inv_mutant!("common/cas-accepts-stale-version")));
        let v = rt.block_on(versions_never_reused(&m, "ver")).expect("ver");
        let sb = v.same_bytes.expect("same-bytes conclusive");
        assert!(
            !sb.ok,
            "the sub-check held under the CAS mutant: {}",
            v.details
        );
        assert!(!sb.reused, "{}", v.details);

        // When V2 == V1, as on RustFS, the mutant's overwrite is answered Ok
        // and stores Y, which a valid CAS would too: the sub-check holds, and
        // only the different-bytes check refutes the mutant.
        let cvm = Arc::new(BrokenStore::new(
            Arc::new(QuirkStore {
                inner: Arc::new(MemoryStore::new()),
                quirk: PutQuirk::ContentVersion,
            }),
            Some(inv_mutant!("common/cas-accepts-stale-version")),
        ));
        let v = rt
            .block_on(versions_never_reused(&cvm, "ver"))
            .expect("ver");
        let sb = v.same_bytes.expect("same-bytes conclusive");
        assert!(sb.reused && sb.ok, "{}", v.details);
        assert_eq!(
            v.details["same_bytes"]["cas"],
            json!(format!("{:?}", Put::Ok(content_version(&body("ver", "y")))))
        );
        assert!(!v.holds, "{}", v.details);
    }

    #[test]
    fn same_bytes_cas_that_landed_but_was_not_answered_ok_is_judged_by_reuse() {
        let rt = rt();
        type Answer = fn() -> StoreError;
        let answers: [(&str, Answer); 2] = [
            ("PreconditionFailed", || StoreError::PreconditionFailed),
            ("Failed", || StoreError::Timeout),
        ];
        for (name, answer) in answers {
            // V2 == V1, so CAS(V1, Y) is a valid CAS; it landed and its
            // response was lost or retried. The key holds Y, which is
            // conformant.
            let s = QuirkStore::wrapped(PutQuirk::SameCasLandsThenAnswered {
                reuse: true,
                answer,
            });
            let v = rt.block_on(versions_never_reused(&s, "ver")).expect("ver");
            let sb = v.same_bytes.expect("same-bytes conclusive");
            assert!(sb.reused, "{name}: {}", v.details);
            assert!(sb.ok, "{name}: false red on reuse: {}", v.details);
            assert!(
                v.details["same_bytes"]["cas"]
                    .as_str()
                    .is_some_and(|c| c.starts_with(name)),
                "{name}: {}",
                v.details
            );
            assert!(v.holds, "{name}: {}", v.details);

            // V2 != V1: the CAS held a stale version, so a store that applied
            // it is wrong whatever it answered.
            let s = QuirkStore::wrapped(PutQuirk::SameCasLandsThenAnswered {
                reuse: false,
                answer,
            });
            let v = rt.block_on(versions_never_reused(&s, "ver")).expect("ver");
            let sb = v.same_bytes.expect("same-bytes conclusive");
            assert!(!sb.reused, "{name}: {}", v.details);
            assert!(!sb.ok, "{name}: stale CAS applied but held: {}", v.details);
        }
    }

    #[test]
    fn a_fault_in_the_same_bytes_setup_keeps_the_different_bytes_verdict() {
        let rt = rt();
        for step in [Op::Put, Op::Delete] {
            // The CAS mutant breaks the different-bytes check; a fault in the
            // same-bytes setup that follows must not hide that violation.
            let rule = Rule::new(step, ScriptedFault::Transient("injected".to_string()))
                .with_key_contains("/same");
            let faults = Arc::new(FaultStore::new(
                MemoryStore::new(),
                FaultPlan::empty().with_rule(rule),
            ));
            let s = BrokenStore::new(
                faults.clone(),
                Some(inv_mutant!("common/cas-accepts-stale-version")),
            );
            let v = rt
                .block_on(versions_never_reused(&s, "ver"))
                .expect("the different-bytes verdict is reported");
            assert!(!v.holds, "{step:?}: {}", v.details);
            assert!(v.same_bytes.is_none(), "{step:?}: {}", v.details);
            assert!(
                v.details["same_bytes"]["inconclusive"].is_string(),
                "{step:?}: {}",
                v.details
            );
            assert_eq!(faults.fault_count(step, FaultKind::Transient), 1);
        }
    }

    #[test]
    fn cas_applied_then_retried_precondition_failed_is_not_a_false_red() {
        let rt = rt();
        // The fresh CAS applies its bytes but is answered PreconditionFailed,
        // as object_store's retry of a CAS that already landed is. A re-read
        // shows the CAS's own bytes, so the outcome matches the effect and the
        // property must hold rather than red.
        let s = QuirkStore::wrapped(PutQuirk::AppliesThenRefusesFirstCas(AtomicUsize::new(0)));
        let v = rt.block_on(cas(&s, "cas")).expect("cas");
        assert!(
            v.holds,
            "false red on an applied-then-retried CAS: {}",
            v.details
        );
        assert_eq!(v.details["fresh"]["ok"], json!(true));
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
                .put(
                    "tr/k",
                    Bytes::from_static(b"tr/t trunc"),
                    PutOptions::default(),
                )
                .await
                .expect("plant");
            let v = transient(&hurried, &reader, "tr", 64)
                .await
                .expect("transient");
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
