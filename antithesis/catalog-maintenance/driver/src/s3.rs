//! Raw S3 calls the driver makes around ravel: listing the bucket for the
//! audit, the seed marker, and HEAD corruption. Each call is one `curl
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
    /// The continuation token when the listing is truncated.
    pub next: Option<String>,
}

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
        let url = format!("{}/{}{}", self.endpoint, self.bucket, path_and_query);
        let n = SCRATCH.fetch_add(1, Ordering::SeqCst);
        let out = std::env::temp_dir().join(format!("s3-{}-{n}.out", std::process::id()));
        let input = std::env::temp_dir().join(format!("s3-{}-{n}.in", std::process::id()));
        let mut cmd = Command::new("curl");
        cmd.args(["-s", "--max-time", "30", "-X", method])
            .args(["--aws-sigv4", &self.sigv4, "--user", &self.user])
            .arg("-o")
            .arg(&out)
            .args(["-w", "%{http_code}"]);
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
        let mut keys = Vec::new();
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
            keys.extend(page.keys);
            match page.next {
                Some(t) => token = Some(t),
                None => return Ok(keys),
            }
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
        next,
    })
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
                   <Contents><Key>t/ab/l/c/0/1/w.1.2.cmt</Key></Contents>\
                   <Contents><Key>a&amp;b</Key></Contents>\
                   <NextContinuationToken>tok/1=</NextContinuationToken></ListBucketResult>";
        assert_eq!(
            tag_values(xml, "Key"),
            vec!["t/ab/l/c/0/1/w.1.2.cmt", "a&b"]
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
                next: None,
            })
        );
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
