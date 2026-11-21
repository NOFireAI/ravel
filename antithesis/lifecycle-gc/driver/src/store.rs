//! Raw S3 calls the driver makes beside Ravel: the ready marker, and the
//! listings and GETs it observes erasure and GC progress through. Each call
//! is one `curl --aws-sigv4` subprocess, so the driver needs no S3 SDK. A
//! listing that does not complete is an error, never an empty bucket.

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Store {
    endpoint: String,
    bucket: String,
    sigv4: String,
    user: String,
}

struct Response {
    status: u16,
    body: Vec<u8>,
    complete: bool,
}

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
        let n = SCRATCH.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir();
        let out = dir.join(format!("lc-s3-{}-{n}.out", std::process::id()));
        let input = dir.join(format!("lc-s3-{}-{n}.in", std::process::id()));
        let mut cmd = Command::new("curl");
        cmd.args(["-s", "--max-time", "30"]);
        if method == "HEAD" {
            cmd.arg("-I");
        } else {
            cmd.args(["-X", method]);
        }
        cmd.args(["--aws-sigv4", &self.sigv4, "--user", &self.user])
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
        cmd.arg(format!(
            "{}/{}{}",
            self.endpoint, self.bucket, path_and_query
        ));
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

    /// Every key under `prefix`, walking ListObjectsV2 pages.
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
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                }
            };
            keys.extend(page.0);
            match page.1 {
                Some(t) => token = Some(t),
                None => return Ok(keys),
            }
        }
    }

    /// Whether the object exists: `Ok(false)` only on a 404.
    pub fn exists(&self, key: &str) -> Result<bool, String> {
        let r = self.request("HEAD", &format!("/{key}"), None);
        match (r.complete, r.status) {
            (true, 200) => Ok(true),
            (true, 404) => Ok(false),
            (c, s) => Err(format!("HEAD {key} answered {s} (complete: {c})")),
        }
    }

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
            s => Err(format!("PUT {key} answered {s}")),
        }
    }

    /// Delete the object; a key already absent counts as deleted.
    pub fn delete(&self, key: &str) -> Result<(), String> {
        let r = self.request("DELETE", &format!("/{key}"), None);
        match (r.complete, r.status) {
            (true, 200 | 204 | 404) => Ok(()),
            (c, s) => Err(format!("DELETE {key} answered {s} (complete: {c})")),
        }
    }
}

/// One ListObjectsV2 response: its keys and continuation token. Only a
/// finished transfer answered 200 whose body closes `</ListBucketResult>`
/// counts; anything else could be a cut-off prefix of the keys.
fn parse_list_page(
    complete: bool,
    status: u16,
    xml: &str,
) -> Result<(Vec<String>, Option<String>), String> {
    if !complete {
        return Err(format!("curl did not finish (status {status})"));
    }
    if status != 200 {
        return Err(format!("answered {status}"));
    }
    if !xml.trim_end().ends_with("</ListBucketResult>") {
        return Err(format!("truncated body ({} bytes)", xml.len()));
    }
    let truncated = tag_values(xml, "IsTruncated").first().map(String::as_str) == Some("true");
    let next = if truncated {
        Some(
            tag_values(xml, "NextContinuationToken")
                .into_iter()
                .next()
                .ok_or("truncated with no continuation token")?,
        )
    } else {
        None
    };
    Ok((tag_values(xml, "Key"), next))
}

fn encode(s: &str) -> String {
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

fn tag_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else {
            break;
        };
        out.push(
            after[..j]
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&amp;", "&"),
        );
        rest = &after[j + close.len()..];
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn only_a_complete_page_is_a_listing() {
        let page = "<ListBucketResult><IsTruncated>false</IsTruncated>\
                    <Contents><Key>t/a/l/del/x.dreq</Key></Contents></ListBucketResult>";
        let (keys, next) = parse_list_page(true, 200, page).expect("page");
        assert_eq!((keys, next), (vec!["t/a/l/del/x.dreq".to_string()], None));
        assert!(parse_list_page(false, 200, page).is_err());
        assert!(parse_list_page(true, 503, page).is_err());
        assert!(parse_list_page(true, 200, &page[..page.len() - 5]).is_err());
        let trunc = page.replace(">false<", ">true<");
        assert!(parse_list_page(true, 200, &trunc).is_err());
        assert_eq!(encode("t/a b="), "t%2Fa%20b%3D");
    }
}
