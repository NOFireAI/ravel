//! The S3 proxy between the ravel-server containers and rustfs (README.md,
//! "The forced races"). It forwards every byte unchanged, Host header
//! included, so each SigV4 signature still verifies at rustfs. It parses only
//! request heads, to frame each request's body and to match its method and
//! key against the rules the driver sets over the control port:
//!
//! - `hold-record`: hold the next PUT of a compaction record, unsent, until
//!   `release` or [`HOLD_CAP`], whichever comes first. This is a delay of one
//!   request in the network, after the sender's last LIST.
//! - `refuse-records`, `refuse-key <key>`: answer every PUT of a compaction
//!   record, or of that key, `503 SlowDown` without forwarding it, as a store
//!   failing those writes would.
//! - `held`: `held <key> <age_ms>`, or `none`.
//! - `release`, `pass`: release the hold; `pass` also lifts every rule.
//!
//! A restarted proxy starts with no rules, so a proxy killed mid-race
//! leaves the servers on the plain path.

use std::collections::BTreeSet;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::keys;

/// The longest a held request waits for `release`: below the 20-second
/// request timeout of the servers' store client, so a hold the driver never
/// releases still reaches rustfs while the server waits for the answer.
pub const HOLD_CAP: Duration = Duration::from_secs(18);
/// The largest request head the proxy reads.
const MAX_HEAD: usize = 64 * 1024;

#[derive(Default)]
struct State {
    hold_next_record: bool,
    held: Option<(String, Instant)>,
    /// Bumped by every release, so a held request knows it was released.
    releases: u64,
    refuse_records: bool,
    refuse_keys: BTreeSet<String>,
}

/// What happens to one request.
#[derive(Debug, PartialEq, Eq)]
pub enum Admit {
    Forward,
    Refuse,
}

#[derive(Default)]
pub struct Rules {
    state: Mutex<State>,
    released: Condvar,
}

impl Rules {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Apply one control command and return the reply line.
    pub fn apply(&self, command: &str) -> String {
        let mut s = self.lock();
        let (verb, arg) = command.trim().split_once(' ').unwrap_or((command.trim(), ""));
        match (verb, arg) {
            ("pass", "") => {
                *s = State {
                    held: s.held.take(),
                    releases: s.releases + 1,
                    ..State::default()
                };
                self.released.notify_all();
            }
            ("hold-record", "") => s.hold_next_record = true,
            ("refuse-records", "") => s.refuse_records = true,
            ("refuse-key", key) if !key.is_empty() => {
                s.refuse_keys.insert(key.to_string());
            }
            ("release", "") => {
                s.hold_next_record = false;
                s.releases += 1;
                self.released.notify_all();
            }
            ("held", "") => {
                return match &s.held {
                    Some((key, since)) => format!("held {key} {}", since.elapsed().as_millis()),
                    None => "none".to_string(),
                };
            }
            _ => return format!("error: unknown command {:?}", command.trim()),
        }
        "ok".to_string()
    }

    /// The verdict on one request for `key`. A held request blocks here
    /// until released or `cap` passes, then is forwarded.
    pub fn admit(&self, method: &str, key: &str, cap: Duration) -> Admit {
        if method != "PUT" {
            return Admit::Forward;
        }
        let record = keys::parse_record_key(key).is_some();
        let mut s = self.lock();
        if s.refuse_keys.contains(key) || (record && s.refuse_records) {
            return Admit::Refuse;
        }
        if !(record && s.hold_next_record && s.held.is_none()) {
            return Admit::Forward;
        }
        s.hold_next_record = false;
        s.held = Some((key.to_string(), Instant::now()));
        let releases = s.releases;
        let deadline = Instant::now() + cap;
        while s.releases == releases {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            s = self
                .released
                .wait_timeout(s, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        s.held = None;
        Admit::Forward
    }
}

/// How a request's body is framed.
#[derive(Debug, PartialEq, Eq)]
pub enum Body {
    None,
    Length(u64),
    Chunked,
}

/// One request head, with its bytes as read.
#[derive(Debug)]
pub struct Head {
    pub method: String,
    pub target: String,
    pub body: Body,
    pub raw: Vec<u8>,
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Read one request head; `None` on a clean end of stream before it.
pub fn read_head(r: &mut impl BufRead) -> io::Result<Option<Head>> {
    let mut raw = Vec::new();
    let mut lines = Vec::new();
    loop {
        let mut line = Vec::new();
        let n = r.take((MAX_HEAD + 1 - raw.len()) as u64).read_until(b'\n', &mut line)?;
        if n == 0 {
            if raw.is_empty() {
                return Ok(None);
            }
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        raw.extend_from_slice(&line);
        if raw.len() > MAX_HEAD {
            return Err(invalid("request head too large".to_string()));
        }
        let text = String::from_utf8_lossy(&line)
            .trim_end_matches(['\r', '\n'])
            .to_string();
        if text.is_empty() {
            if lines.is_empty() {
                // Blank lines before a request line are skipped (RFC 9112 2.2).
                raw.clear();
                continue;
            }
            break;
        }
        lines.push(text);
    }
    let mut request = lines[0].split(' ');
    let (Some(method), Some(target)) = (request.next(), request.next()) else {
        return Err(invalid(format!("bad request line {:?}", lines[0])));
    };
    let mut body = Body::None;
    for line in &lines[1..] {
        let Some((name, value)) = line.split_once(':') else {
            return Err(invalid(format!("bad header line {line:?}")));
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("transfer-encoding") {
            if value
                .rsplit(',')
                .next()
                .is_some_and(|c| c.trim().eq_ignore_ascii_case("chunked"))
            {
                body = Body::Chunked;
            }
        } else if name.eq_ignore_ascii_case("content-length") && body != Body::Chunked {
            let n = value
                .parse()
                .map_err(|_| invalid(format!("bad content-length {value:?}")))?;
            body = Body::Length(n);
        }
    }
    Ok(Some(Head {
        method: method.to_string(),
        target: target.to_string(),
        body,
        raw,
    }))
}

/// The object key a path-style request target names in `bucket`, decoded;
/// empty for a request on the bucket itself.
pub fn key_of(target: &str, bucket: &str) -> String {
    let path = target.split('?').next().unwrap_or("");
    let path = path
        .strip_prefix("http://")
        .and_then(|rest| rest.find('/').map(|i| &rest[i..]))
        .unwrap_or(path);
    let path = path.strip_prefix('/').unwrap_or(path);
    let key = path
        .strip_prefix(bucket)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or("");
    percent_decode(key)
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = |c: u8| char::from(c).to_digit(16);
        if b[i] == b'%'
            && let (Some(hi), Some(lo)) = (
                b.get(i + 1).copied().and_then(hex),
                b.get(i + 2).copied().and_then(hex),
            )
        {
            out.push(u8::try_from(hi * 16 + lo).unwrap_or(b'?'));
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Copy one request body, framed as `body` says, from `r` to `w`.
pub fn copy_body(r: &mut impl BufRead, w: &mut impl Write, body: &Body) -> io::Result<()> {
    match body {
        Body::None => Ok(()),
        Body::Length(n) => {
            let copied = io::copy(&mut r.take(*n), w)?;
            if copied == *n {
                Ok(())
            } else {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
        }
        Body::Chunked => loop {
            let mut line = Vec::new();
            if r.read_until(b'\n', &mut line)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            w.write_all(&line)?;
            let text = String::from_utf8_lossy(&line);
            let size = text.split(';').next().unwrap_or("").trim();
            let size = u64::from_str_radix(size, 16)
                .map_err(|_| invalid(format!("bad chunk size {size:?}")))?;
            if size == 0 {
                // Trailers, up to the blank line.
                loop {
                    let mut t = Vec::new();
                    if r.read_until(b'\n', &mut t)? == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    w.write_all(&t)?;
                    if t == b"\r\n" || t == b"\n" {
                        return Ok(());
                    }
                }
            }
            // The chunk and its CRLF.
            let want = size + 2;
            if io::copy(&mut r.take(want), w)? != want {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        },
    }
}

/// The answer to a refused request.
pub fn refusal() -> Vec<u8> {
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>\
                <Message>refused by the test proxy</Message></Error>";
    format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/xml\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Relay one client connection to `upstream`.
pub fn relay(client: TcpStream, upstream: &str, bucket: &str, rules: &Rules) -> io::Result<()> {
    let up = TcpStream::connect(upstream)?;
    let _ = up.set_nodelay(true);
    let _ = client.set_nodelay(true);
    let mut up_read = up.try_clone()?;
    let mut client_write = client.try_clone()?;
    let answers = std::thread::spawn(move || {
        let _ = io::copy(&mut up_read, &mut client_write);
        // The upstream closed: so does the client, which ends the request
        // loop below.
        let _ = client_write.shutdown(Shutdown::Both);
    });
    let result = requests(&client, &up, bucket, rules);
    let _ = up.shutdown(Shutdown::Both);
    let _ = client.shutdown(Shutdown::Both);
    let _ = answers.join();
    result
}

fn requests(client: &TcpStream, up: &TcpStream, bucket: &str, rules: &Rules) -> io::Result<()> {
    let mut reader = BufReader::new(client.try_clone()?);
    let mut up_write = up.try_clone()?;
    while let Some(head) = read_head(&mut reader)? {
        let key = key_of(&head.target, bucket);
        match rules.admit(&head.method, &key, HOLD_CAP) {
            Admit::Refuse => {
                // No pipelining: the previous answer has been read already.
                let _ = up.shutdown(Shutdown::Both);
                let mut w = client.try_clone()?;
                w.write_all(&refusal())?;
                w.flush()?;
                return Ok(());
            }
            Admit::Forward => {
                up_write.write_all(&head.raw)?;
                copy_body(&mut reader, &mut up_write, &head.body)?;
                up_write.flush()?;
            }
        }
    }
    Ok(())
}

/// Accept connections on `listener` forever, one thread each.
pub fn serve(listener: TcpListener, upstream: String, bucket: String, rules: Arc<Rules>) {
    for conn in listener.incoming() {
        let Ok(client) = conn else {
            continue;
        };
        let (upstream, bucket, rules) = (upstream.clone(), bucket.clone(), Arc::clone(&rules));
        std::thread::spawn(move || {
            if let Err(e) = relay(client, &upstream, &bucket, &rules) {
                eprintln!("s3-proxy: connection ended: {e}");
            }
        });
    }
}

/// Serve the control port forever: one command per line, one reply line
/// each.
pub fn serve_control(listener: TcpListener, rules: Arc<Rules>) {
    for conn in listener.incoming() {
        let Ok(stream) = conn else {
            continue;
        };
        let rules = Arc::clone(&rules);
        std::thread::spawn(move || {
            let Ok(mut w) = stream.try_clone() else {
                return;
            };
            for line in BufReader::new(stream).lines() {
                let Ok(line) = line else {
                    return;
                };
                let reply = rules.apply(&line);
                println!("s3-proxy: {} -> {reply}", line.trim());
                if writeln!(w, "{reply}").is_err() {
                    return;
                }
            }
        });
    }
}

/// Send one command to the proxy's control port at `addr` and return its
/// reply; an error when the proxy does not answer, which a killed proxy
/// does not.
pub fn control(addr: &str, command: &str) -> Result<String, String> {
    let sock = addr
        .to_socket_addrs()
        .map_err(|e| format!("{addr}: {e}"))?
        .next()
        .ok_or_else(|| format!("{addr} resolves to nothing"))?;
    let mut stream = TcpStream::connect_timeout(&sock, Duration::from_secs(2))
        .map_err(|e| format!("{addr}: {e}"))?;
    let timeout = Some(Duration::from_secs(5));
    stream
        .set_read_timeout(timeout)
        .and_then(|()| stream.set_write_timeout(timeout))
        .map_err(|e| format!("{addr}: {e}"))?;
    writeln!(stream, "{command}").map_err(|e| format!("{addr}: {e}"))?;
    let mut reply = String::new();
    BufReader::new(stream)
        .read_line(&mut reply)
        .map_err(|e| format!("{addr}: {e}"))?;
    let reply = reply.trim().to_string();
    if reply.is_empty() || reply.starts_with("error") {
        return Err(format!("{addr}: {command}: {reply:?}"));
    }
    Ok(reply)
}

/// The key and age of a `held` reply.
pub fn parse_held(reply: &str) -> Option<(String, Duration)> {
    let rest = reply.strip_prefix("held ")?;
    let (key, ms) = rest.rsplit_once(' ')?;
    Some((key.to_string(), Duration::from_millis(ms.parse().ok()?)))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const RECORD: &str = "t/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt";

    fn head(text: &str) -> io::Result<Option<Head>> {
        read_head(&mut io::Cursor::new(text.as_bytes().to_vec()))
    }

    #[test]
    fn request_heads_frame_their_bodies() {
        let h = head("PUT /ravel/a/b HTTP/1.1\r\nHost: s3proxy:9000\r\nContent-Length: 5\r\n\r\nhello")
            .expect("parses")
            .expect("a head");
        assert_eq!((h.method.as_str(), h.target.as_str()), ("PUT", "/ravel/a/b"));
        assert_eq!(h.body, Body::Length(5));
        assert!(h.raw.ends_with(b"\r\n\r\n"));
        assert!(!h.raw.ends_with(b"hello"));
        let h = head("PUT /ravel/a HTTP/1.1\r\ntransfer-encoding: gzip, chunked\r\ncontent-length: 9\r\n\r\n")
            .expect("parses")
            .expect("a head");
        assert_eq!(h.body, Body::Chunked);
        let h = head("\r\nGET /ravel?list-type=2 HTTP/1.1\r\nHost: x\r\n\r\n")
            .expect("parses")
            .expect("a head");
        assert_eq!(h.body, Body::None);
        assert!(h.raw.starts_with(b"GET "));
        assert!(head("").expect("clean end").is_none());
        assert!(head("PUT /ravel/a HTTP/1.1\r\nHost").is_err());
        assert!(head("PUT /ravel/a HTTP/1.1\r\nContent-Length: x\r\n\r\n").is_err());
        let big = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(MAX_HEAD));
        assert!(head(&big).is_err());
    }

    #[test]
    fn bodies_copy_exactly_their_framing() {
        let mut r = io::Cursor::new(b"helloNEXT".to_vec());
        let mut w = Vec::new();
        copy_body(&mut r, &mut w, &Body::Length(5)).expect("copies");
        assert_eq!(w, b"hello");
        let chunked = b"5;x=1\r\nhello\r\n0\r\nT: v\r\n\r\nNEXT";
        let mut r = io::Cursor::new(chunked.to_vec());
        let mut w = Vec::new();
        copy_body(&mut r, &mut w, &Body::Chunked).expect("copies");
        assert_eq!(w, &chunked[..chunked.len() - 4]);
        let mut short = io::Cursor::new(b"hel".to_vec());
        assert!(copy_body(&mut short, &mut Vec::new(), &Body::Length(5)).is_err());
        let mut bad = io::Cursor::new(b"zz\r\n".to_vec());
        assert!(copy_body(&mut bad, &mut Vec::new(), &Body::Chunked).is_err());
    }

    #[test]
    fn keys_come_from_path_style_targets() {
        assert_eq!(key_of(&format!("/ravel/{RECORD}"), "ravel"), RECORD);
        assert_eq!(key_of(&format!("/ravel/{RECORD}?x-id=PutObject"), "ravel"), RECORD);
        assert_eq!(key_of("/ravel/a%2Fb%3D", "ravel"), "a/b=");
        assert_eq!(key_of("/ravel?list-type=2&prefix=t%2F", "ravel"), "");
        assert_eq!(key_of("/ravelx/a", "ravel"), "");
        assert_eq!(key_of("http://s3proxy:9000/ravel/a", "ravel"), "a");
    }

    #[test]
    fn rules_refuse_and_pass() {
        let rules = Rules::default();
        let cap = Duration::from_millis(10);
        assert_eq!(rules.admit("PUT", RECORD, cap), Admit::Forward);
        assert_eq!(rules.apply("refuse-records"), "ok");
        assert_eq!(rules.admit("PUT", RECORD, cap), Admit::Refuse);
        assert_eq!(rules.admit("GET", RECORD, cap), Admit::Forward);
        assert_eq!(rules.admit("PUT", "t/0a0b/catalog/logs/HEAD", cap), Admit::Forward);
        assert_eq!(rules.apply("refuse-key t/0a0b/catalog/logs/HEAD"), "ok");
        assert_eq!(rules.admit("PUT", "t/0a0b/catalog/logs/HEAD", cap), Admit::Refuse);
        assert_eq!(rules.admit("DELETE", "t/0a0b/catalog/logs/HEAD", cap), Admit::Forward);
        assert_eq!(rules.apply("pass"), "ok");
        assert_eq!(rules.admit("PUT", RECORD, cap), Admit::Forward);
        assert_eq!(rules.admit("PUT", "t/0a0b/catalog/logs/HEAD", cap), Admit::Forward);
        assert!(rules.apply("refuse-key").starts_with("error"));
        assert!(rules.apply("bogus").starts_with("error"));
    }

    #[test]
    fn a_hold_takes_one_record_put_until_released() {
        let rules = Arc::new(Rules::default());
        assert_eq!(rules.apply("held"), "none");
        assert_eq!(rules.apply("hold-record"), "ok");
        // Not a record PUT: never held.
        assert_eq!(
            rules.admit("PUT", "t/0a0b/l/c/0003/20261117T16/w1.1.1.cmt", HOLD_CAP),
            Admit::Forward
        );
        let r = Arc::clone(&rules);
        let started = Instant::now();
        let held = std::thread::spawn(move || r.admit("PUT", RECORD, HOLD_CAP));
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut reply = rules.apply("held");
        while reply == "none" && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
            reply = rules.apply("held");
        }
        let (key, _) = parse_held(&reply).expect("a held reply");
        assert_eq!(key, RECORD);
        // The hold is one-shot: a second record PUT goes straight through.
        assert_eq!(rules.admit("PUT", RECORD, HOLD_CAP), Admit::Forward);
        assert_eq!(rules.apply("release"), "ok");
        assert_eq!(held.join().expect("joins"), Admit::Forward);
        assert!(started.elapsed() < HOLD_CAP);
        assert_eq!(rules.apply("held"), "none");
    }

    #[test]
    fn an_unreleased_hold_ends_at_its_cap() {
        let rules = Rules::default();
        rules.apply("hold-record");
        let started = Instant::now();
        assert_eq!(rules.admit("PUT", RECORD, Duration::from_millis(50)), Admit::Forward);
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert_eq!(rules.apply("held"), "none");
    }

    #[test]
    fn held_replies_parse() {
        assert_eq!(
            parse_held(&format!("held {RECORD} 1500")),
            Some((RECORD.to_string(), Duration::from_millis(1500)))
        );
        assert_eq!(parse_held("none"), None);
        assert_eq!(parse_held("held x y"), None);
    }

    /// A fake rustfs: answers each request on each connection `200` with no
    /// body and records every byte it was sent.
    fn upstream() -> (String, Arc<Mutex<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let addr = listener.local_addr().expect("an address").to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { continue };
                let log = Arc::clone(&log);
                std::thread::spawn(move || {
                    let mut w = conn.try_clone().expect("clones");
                    let mut r = BufReader::new(conn);
                    while let Ok(Some(h)) = read_head(&mut r) {
                        let mut body = Vec::new();
                        copy_body(&mut r, &mut body, &h.body).expect("a body");
                        let mut l = log.lock().expect("locks");
                        l.extend_from_slice(&h.raw);
                        l.extend_from_slice(&body);
                        drop(l);
                        w.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .expect("answers");
                    }
                });
            }
        });
        (addr, seen)
    }

    fn proxy(up: String, rules: Arc<Rules>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let addr = listener.local_addr().expect("an address").to_string();
        std::thread::spawn(move || serve(listener, up, "ravel".to_string(), rules));
        addr
    }

    fn answer(r: &mut impl BufRead) -> String {
        let mut text = String::new();
        loop {
            let mut line = String::new();
            if r.read_line(&mut line).expect("reads") == 0 || line == "\r\n" {
                return text;
            }
            text.push_str(&line);
        }
    }

    #[test]
    fn requests_are_relayed_byte_for_byte_and_refused_unsent() {
        let (up, seen) = upstream();
        let rules = Arc::new(Rules::default());
        let addr = proxy(up, Arc::clone(&rules));
        let mut conn = TcpStream::connect(&addr).expect("connects");
        let mut r = BufReader::new(conn.try_clone().expect("clones"));
        let put = format!(
            "PUT /ravel/{RECORD} HTTP/1.1\r\nHost: s3proxy:9000\r\nContent-Length: 3\r\n\r\nabc"
        );
        let chunked = "PUT /ravel/x HTTP/1.1\r\nHost: s3proxy:9000\r\n\
                       Transfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n";
        // Two requests on one kept-alive connection.
        conn.write_all(put.as_bytes()).expect("writes");
        assert!(answer(&mut r).starts_with("HTTP/1.1 200"));
        conn.write_all(chunked.as_bytes()).expect("writes");
        assert!(answer(&mut r).starts_with("HTTP/1.1 200"));
        assert_eq!(
            String::from_utf8(seen.lock().expect("locks").clone()).expect("text"),
            format!("{put}{chunked}")
        );

        rules.apply("refuse-records");
        let mut conn = TcpStream::connect(&addr).expect("connects");
        conn.write_all(put.as_bytes()).expect("writes");
        let mut refusal = Vec::new();
        conn.read_to_end(&mut refusal).expect("reads to the close");
        assert_eq!(refusal, super::refusal());
        let text = String::from_utf8(refusal).expect("text");
        assert!(text.starts_with("HTTP/1.1 503 "));
        let (head, body) = text.split_once("\r\n\r\n").expect("a head end");
        assert!(head.contains(&format!("\r\nContent-Length: {}\r\n", body.len())));
        assert!(body.contains("<Code>SlowDown</Code>"));
        // Nothing more reached the upstream.
        assert_eq!(seen.lock().expect("locks").len(), put.len() + chunked.len());
    }

    #[test]
    fn the_control_port_sets_the_rules() {
        let rules = Arc::new(Rules::default());
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let addr = listener.local_addr().expect("an address").to_string();
        let r = Arc::clone(&rules);
        std::thread::spawn(move || serve_control(listener, r));
        assert_eq!(control(&addr, "refuse-records"), Ok("ok".to_string()));
        assert_eq!(rules.admit("PUT", RECORD, HOLD_CAP), Admit::Refuse);
        assert_eq!(control(&addr, "held"), Ok("none".to_string()));
        assert!(control(&addr, "bogus").is_err());
        assert_eq!(control(&addr, "pass"), Ok("ok".to_string()));
        assert_eq!(rules.admit("PUT", RECORD, HOLD_CAP), Admit::Forward);
        assert!(control("127.0.0.1:1", "held").is_err());
    }
}
