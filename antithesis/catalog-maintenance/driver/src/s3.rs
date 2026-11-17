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
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

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
                };
            }
            cmd.arg("--data-binary")
                .arg(format!("@{}", input.display()));
        }
        cmd.arg(&url);
        let status = cmd
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
            .unwrap_or(0);
        let body = std::fs::read(&out).unwrap_or_default();
        let _ = std::fs::remove_file(&out);
        let _ = std::fs::remove_file(&input);
        Response { status, body }
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
            let r = self.request("GET", &query, None);
            let xml = String::from_utf8_lossy(&r.body);
            if r.status != 200 {
                return Err(format!("LIST {prefix} answered {}: {xml}", r.status));
            }
            keys.extend(tag_values(&xml, "Key"));
            let truncated =
                tag_values(&xml, "IsTruncated").first().map(String::as_str) == Some("true");
            if !truncated {
                return Ok(keys);
            }
            match tag_values(&xml, "NextContinuationToken").into_iter().next() {
                Some(t) => token = Some(t),
                None => {
                    return Err(format!(
                        "LIST {prefix} is truncated with no continuation token"
                    ));
                }
            }
        }
    }

    /// The object's bytes, `None` when it does not exist.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let r = self.request("GET", &format!("/{key}"), None);
        match r.status {
            200 => Ok(Some(r.body)),
            404 => Ok(None),
            s => Err(format!("GET {key} answered {s}")),
        }
    }

    /// An unconditional PUT.
    pub fn put(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let r = self.request("PUT", &format!("/{key}"), Some(bytes));
        match r.status {
            200 => Ok(()),
            s => Err(format!(
                "PUT {key} answered {s}: {}",
                String::from_utf8_lossy(&r.body)
            )),
        }
    }
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
}
