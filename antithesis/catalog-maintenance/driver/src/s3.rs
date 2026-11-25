//! Raw S3 calls the driver makes around ravel: listing the bucket for the
//! audit, the seed marker, HEAD corruption, and the record races' tampers.
//! Each call is one `curl
//! --aws-sigv4` subprocess, the same client the compose entrypoints use, so
//! the driver needs no S3 SDK.

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Store {
    endpoint: String,
    bucket: String,
    sigv4: String,
    user: String,
}

/// The status and body of one request; `status` is 0 when curl got no answer.
/// `complete` is whether curl exited 0: a transfer cut off by the timeout or
/// a reset still reports the status line it read, with a partial body.
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    pub complete: bool,
}

/// One ListObjectsV2 page.
#[derive(Debug, PartialEq, Eq)]
pub struct ListPage {
    pub keys: Vec<String>,
    /// Each listed object's key with its `LastModified` in unix
    /// milliseconds, `None` when that does not parse.
    pub stamped: Vec<(String, Option<i64>)>,
    /// The continuation token when the listing is truncated.
    pub next: Option<String>,
}

/// The longest one request may take: curl's `--max-time`.
pub const MAX_TIME_S: u64 = 30;

/// How often one listing page is requested before the listing fails.
const LIST_PAGE_ATTEMPTS: u32 = 3;

static SCRATCH: AtomicU64 = AtomicU64::new(0);

impl Store {
    pub fn from_env() -> Result<Store, String> {
        let var = |k: &str| std::env::var(k).map_err(|_| format!("{k} is not set"));
        Ok(Store {
            endpoint: var("RAVEL_S3_ENDPOINT")?.trim_end_matches('/').to_string(),
            bucket: var("RAVEL_S3_BUCKET")?,
            sigv4: format!("aws:amz:{}:s3", var("RAVEL_S3_REGION")?),
            user: format!(
                "{}:{}",
                var("RAVEL_S3_ACCESS_KEY")?,
                var("RAVEL_S3_SECRET_KEY")?
            ),
        })
    }

    fn request(&self, method: &str, path_and_query: &str, body: Option<&[u8]>) -> Response {
        self.request_with(method, path_and_query, body, &[])
    }

    /// [`Store::request`] with extra request headers, each `Name: value`,
    /// which curl signs with the rest. A HEAD is sent with `-I`, since `-X
    /// HEAD` waits for a body to `--max-time`.
    fn request_with(
        &self,
        method: &str,
        path_and_query: &str,
        body: Option<&[u8]>,
        headers: &[String],
    ) -> Response {
        let url = format!("{}/{}{}", self.endpoint, self.bucket, path_and_query);
        let n = SCRATCH.fetch_add(1, Ordering::SeqCst);
        let out = std::env::temp_dir().join(format!("s3-{}-{n}.out", std::process::id()));
        let input = std::env::temp_dir().join(format!("s3-{}-{n}.in", std::process::id()));
        let mut cmd = Command::new("curl");
        let max_time = MAX_TIME_S.to_string();
        cmd.args(["-s", "--max-time", &max_time]);
        if method == "HEAD" {
            cmd.arg("-I");
        } else {
            cmd.args(["-X", method]);
        }
        cmd.args(["--aws-sigv4", &self.sigv4, "--user", &self.user])
            .arg("-o")
            .arg(&out)
            .args(["-w", "%{http_code}"]);
        for h in headers {
            cmd.arg("-H").arg(h);
        }
        if let Some(bytes) = body {
            if std::fs::write(&input, bytes).is_err() {
                return Response {
                    status: 0,
                    body: Vec::new(),
                    complete: false,
                };
            }
            cmd.arg("--data-binary")
                .arg(format!("@{}", input.display()));
        }
        cmd.arg(&url);
        let (status, complete) = match cmd.output() {
            Ok(o) => (
                String::from_utf8_lossy(&o.stdout)
                    .trim()
                    .parse()
                    .unwrap_or(0),
                o.status.success(),
            ),
            Err(_) => (0, false),
        };
        let body = std::fs::read(&out).unwrap_or_default();
        let _ = std::fs::remove_file(&out);
        let _ = std::fs::remove_file(&input);
        Response {
            status,
            body,
            complete,
        }
    }

    /// Every key under `prefix`, walking ListObjectsV2 pages. A page that
    /// does not parse as complete is requested again, up to
    /// `LIST_PAGE_ATTEMPTS` times; after that the listing fails, and a caller
    /// treats a failed listing as no answer, never as an empty bucket.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        Ok(self
            .list_pages(prefix)?
            .into_iter()
            .flat_map(|page| page.keys)
            .collect())
    }

    /// [`Store::list`] with each key's `LastModified`, in unix milliseconds
    /// (`None` when the listing's stamp does not parse).
    pub fn list_stamped(&self, prefix: &str) -> Result<Vec<(String, Option<i64>)>, String> {
        Ok(self
            .list_pages(prefix)?
            .into_iter()
            .flat_map(|page| page.stamped)
            .collect())
    }

    fn list_pages(&self, prefix: &str) -> Result<Vec<ListPage>, String> {
        let mut pages = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = format!("?list-type=2&prefix={}", encode(prefix));
            if let Some(t) = &token {
                query.push_str(&format!("&continuation-token={}", encode(t)));
            }
            let mut attempt = 1;
            let page = loop {
                let r = self.request("GET", &query, None);
                match parse_list_page(r.complete, r.status, &String::from_utf8_lossy(&r.body)) {
                    Ok(page) => break page,
                    Err(e) if attempt >= LIST_PAGE_ATTEMPTS => {
                        return Err(format!("LIST {prefix}: {e}"));
                    }
                    Err(_) => {
                        attempt += 1;
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
            };
            let next = page.next.clone();
            pages.push(page);
            match next {
                Some(t) => token = Some(t),
                None => return Ok(pages),
            }
        }
    }

    /// Whether the object exists, by a HEAD.
    pub fn exists(&self, key: &str) -> Result<bool, String> {
        let r = self.request("HEAD", &format!("/{key}"), None);
        if !r.complete {
            return Err(format!(
                "HEAD {key}: curl did not finish (status {})",
                r.status
            ));
        }
        match r.status {
            200 => Ok(true),
            404 => Ok(false),
            s => Err(format!("HEAD {key} answered {s}")),
        }
    }

    /// A create-only PUT (`If-None-Match: *`) carrying `headers` too: `Ok(true)`
    /// when it created the object, `Ok(false)` when the store refused it
    /// because the key exists (412).
    pub fn put_create(&self, key: &str, bytes: &[u8], headers: &[String]) -> Result<bool, String> {
        let mut all = vec!["If-None-Match: *".to_string()];
        all.extend(headers.iter().cloned());
        let r = self.request_with("PUT", &format!("/{key}"), Some(bytes), &all);
        if !r.complete {
            return Err(format!(
                "PUT {key}: curl did not finish (status {})",
                r.status
            ));
        }
        match r.status {
            200 => Ok(true),
            412 => Ok(false),
            s => Err(format!(
                "PUT {key} answered {s}: {}",
                String::from_utf8_lossy(&r.body)
            )),
        }
    }

    /// The object's bytes, `None` when it does not exist.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let r = self.request("GET", &format!("/{key}"), None);
        if !r.complete {
            return Err(format!(
                "GET {key}: curl did not finish (status {})",
                r.status
            ));
        }
        match r.status {
            200 => Ok(Some(r.body)),
            404 => Ok(None),
            s => Err(format!("GET {key} answered {s}")),
        }
    }

    /// An unconditional PUT.
    pub fn put(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let r = self.request("PUT", &format!("/{key}"), Some(bytes));
        if !r.complete {
            return Err(format!(
                "PUT {key}: curl did not finish (status {})",
                r.status
            ));
        }
        match r.status {
            200 => Ok(()),
            s => Err(format!(
                "PUT {key} answered {s}: {}",
                String::from_utf8_lossy(&r.body)
            )),
        }
    }

    /// A DELETE. S3 answers one of an absent key 204 too.
    pub fn delete(&self, key: &str) -> Result<(), String> {
        let r = self.request("DELETE", &format!("/{key}"), None);
        if !r.complete {
            return Err(format!(
                "DELETE {key}: curl did not finish (status {})",
                r.status
            ));
        }
        match r.status {
            200 | 204 => Ok(()),
            s => Err(format!(
                "DELETE {key} answered {s}: {}",
                String::from_utf8_lossy(&r.body)
            )),
        }
    }
}

/// Judge one ListObjectsV2 response. Only a transfer curl finished, answered
/// 200, whose body ends with the closing `</ListBucketResult>`, is a page: a
/// body cut off mid-transfer can hold a prefix of the keys and no
/// `IsTruncated`, which would otherwise read as a complete, shorter listing.
pub fn parse_list_page(complete: bool, status: u16, xml: &str) -> Result<ListPage, String> {
    if !complete {
        return Err(format!(
            "curl did not finish the transfer (status {status})"
        ));
    }
    if status != 200 {
        return Err(format!("answered {status}: {xml}"));
    }
    if !xml.trim_end().ends_with("</ListBucketResult>") {
        return Err(format!(
            "the body does not end with </ListBucketResult> ({} bytes)",
            xml.len()
        ));
    }
    let truncated = tag_values(xml, "IsTruncated").first().map(String::as_str) == Some("true");
    let next = if truncated {
        match tag_values(xml, "NextContinuationToken").into_iter().next() {
            Some(t) => Some(t),
            None => return Err("truncated with no continuation token".to_string()),
        }
    } else {
        None
    };
    Ok(ListPage {
        keys: tag_values(xml, "Key"),
        stamped: stamped_contents(xml),
        next,
    })
}

/// Each `<Contents>` entry's key and `LastModified` (unix milliseconds).
pub fn stamped_contents(xml: &str) -> Vec<(String, Option<i64>)> {
    xml.split("<Contents>")
        .skip(1)
        .filter_map(|entry| {
            let entry = entry.split("</Contents>").next().unwrap_or("");
            let key = tag_values(entry, "Key").into_iter().next()?;
            let stamp = tag_values(entry, "LastModified")
                .first()
                .and_then(|s| last_modified_ms(s));
            Some((key, stamp))
        })
        .collect()
}

/// An S3 `LastModified` stamp, `YYYY-MM-DDTHH:MM:SS[.fff...]Z`, in unix
/// milliseconds, any digits past the millisecond dropped; `None` for anything
/// else.
pub fn last_modified_ms(s: &str) -> Option<i64> {
    let rest = s.strip_suffix('Z')?;
    let (whole, frac) = rest.split_once('.').unwrap_or((rest, ""));
    let b = whole.as_bytes();
    if b.len() != 19
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<u64> {
        let part = whole.get(r)?;
        part.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| part.parse().ok())?
    };
    let hour = ravel_antithesis_catalog_driver::keys::parse_hour_string(&format!(
        "{}{}{}T{}",
        whole.get(0..4)?,
        whole.get(5..7)?,
        whole.get(8..10)?,
        whole.get(11..13)?
    ))?;
    let (min, sec) = (num(14..16)?, num(17..19)?);
    if min > 59 || sec > 59 || !frac.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let ms: u64 = format!("{frac:0<3}").get(0..3)?.parse().ok()?;
    i64::try_from(((hour * 3600 + min * 60 + sec) * 1000) + ms).ok()
}

/// Percent-encode a query value (RFC 3986 unreserved characters kept).
pub fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The text of every `<tag>...</tag>` element, XML entities decoded.
pub fn tag_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else {
            break;
        };
        out.push(unescape(&after[..j]));
        rest = &after[j + close.len()..];
    }
    out
}

fn unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_page_keys_and_token_are_parsed() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
                   <Contents><Key>t/ab/l/l1/0001/20261117T16/0f6eeb04e9886d29.0000.592aa104ca6bae9c.rseg</Key></Contents>\
                   <Contents><Key>a&amp;b</Key></Contents>\
                   <NextContinuationToken>tok/1=</NextContinuationToken></ListBucketResult>";
        assert_eq!(
            tag_values(xml, "Key"),
            vec![
                "t/ab/l/l1/0001/20261117T16/0f6eeb04e9886d29.0000.592aa104ca6bae9c.rseg",
                "a&b"
            ]
        );
        assert_eq!(tag_values(xml, "IsTruncated"), vec!["true"]);
        assert_eq!(encode("tok/1="), "tok%2F1%3D");
    }

    const PAGE: &str = "<?xml version=\"1.0\"?><ListBucketResult>\
                        <IsTruncated>false</IsTruncated>\
                        <Contents><Key>t/ab/a</Key></Contents>\
                        <Contents><Key>t/ab/b</Key></Contents></ListBucketResult>\n";

    #[test]
    fn a_complete_page_is_a_listing() {
        assert_eq!(
            parse_list_page(true, 200, PAGE),
            Ok(ListPage {
                keys: vec!["t/ab/a".to_string(), "t/ab/b".to_string()],
                stamped: vec![("t/ab/a".to_string(), None), ("t/ab/b".to_string(), None)],
                next: None,
            })
        );
    }

    #[test]
    fn listed_stamps_are_read_to_the_millisecond() {
        let xml = "<ListBucketResult><IsTruncated>false</IsTruncated>\
                   <Contents><Key>t/ab/a</Key><LastModified>2026-11-17T16:00:05.123Z</LastModified></Contents>\
                   <Contents><Key>t/ab/b</Key><LastModified>bogus</LastModified></Contents>\
                   <Contents><LastModified>2026-11-17T16:00:05Z</LastModified><Key>t/ab/c</Key></Contents>\
                   </ListBucketResult>";
        assert_eq!(
            stamped_contents(xml),
            vec![
                ("t/ab/a".to_string(), Some(1_794_931_205_123)),
                ("t/ab/b".to_string(), None),
                ("t/ab/c".to_string(), Some(1_794_931_205_000)),
            ]
        );
        assert_eq!(last_modified_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            last_modified_ms("2026-11-17T16:00:05.1Z"),
            Some(1_794_931_205_100)
        );
        assert_eq!(
            last_modified_ms("2026-11-17T16:00:05.123456Z"),
            Some(1_794_931_205_123)
        );
        // A leap day, and the second before the next hour.
        assert_eq!(
            last_modified_ms("2024-02-29T05:59:59.999Z"),
            Some(1_709_186_399_999)
        );
        for bad in [
            "2026-11-17T16:00:05.123",
            "2026-11-17 16:00:05Z",
            "2026-11-17T16:60:05Z",
            "2026-11-17T16:00:60Z",
            "2026-13-17T16:00:05Z",
            "2026-11-17T16:00:05.1x3Z",
            "2026-11-17T16:0a:05Z",
        ] {
            assert_eq!(last_modified_ms(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_cut_off_listing_is_no_listing() {
        // curl timed out or was reset after the status line: it exits
        // non-zero, whatever the body holds.
        assert!(parse_list_page(false, 200, PAGE).is_err());
        // A body cut off after the first key, before IsTruncated would have
        // said more keys follow.
        let cut = &PAGE[..PAGE.find("<Contents><Key>t/ab/b").expect("second key")];
        let cut = cut.replace("<IsTruncated>false</IsTruncated>", "");
        assert!(parse_list_page(true, 200, &cut).is_err());
        assert!(parse_list_page(true, 503, PAGE).is_err());
        let no_token = PAGE.replace(">false<", ">true<");
        assert!(parse_list_page(true, 200, &no_token).is_err());
    }
}
