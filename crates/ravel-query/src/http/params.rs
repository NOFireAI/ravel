//! Hand-rolled query-string and form-body parsing (deliberate deviation:
//! avoids a new dependency purely to support repeated keys like
//! `min_commit_token` and `match[]`, which axum's built-in `Query`
//! extractor does not handle without a bespoke `Deserialize` impl anyway).

use std::collections::HashMap;
use std::time::Duration;

use ravel_types::CommitToken;

#[derive(Debug, thiserror::Error)]
pub enum ParamError {
    #[error("missing required parameter {0:?}")]
    Missing(&'static str),
    #[error("invalid value for parameter {name:?}: {value:?}")]
    Invalid { name: &'static str, value: String },
}

#[derive(Debug, Default)]
pub struct Params {
    values: HashMap<String, Vec<String>>,
}

impl Params {
    pub fn parse(query_string: Option<&str>, body: Option<&[u8]>) -> Self {
        let mut values = query_string.map(parse_form_encoded).unwrap_or_default();
        if let Some(body) = body {
            let body_str = String::from_utf8_lossy(body);
            for (k, vs) in parse_form_encoded(&body_str) {
                values.entry(k).or_default().extend(vs);
            }
        }
        Params { values }
    }

    pub fn first(&self, key: &str) -> Option<&str> {
        self.values.get(key).and_then(|v| v.first()).map(String::as_str)
    }

    pub fn all(&self, key: &str) -> &[String] {
        self.values.get(key).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn require<'a>(&'a self, key: &'static str) -> Result<&'a str, ParamError> {
        self.first(key).ok_or(ParamError::Missing(key))
    }
}

fn parse_form_encoded(s: &str) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let key = percent_decode(parts.next().unwrap_or(""));
        let value = percent_decode(parts.next().unwrap_or(""));
        out.entry(key).or_default().push(value);
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Prometheus float seconds (`1435781451.781`) or RFC3339
/// (`2015-07-01T20:10:51.781Z`). Pre-1970 RFC3339 timestamps are rejected
/// (documented deviation): `SystemTime::duration_since(UNIX_EPOCH)`
/// naturally errors for any instant before the epoch.
pub fn parse_timestamp_ms(name: &'static str, s: &str) -> Result<i64, ParamError> {
    if let Ok(secs) = s.parse::<f64>() {
        return seconds_to_ms(name, s, secs);
    }
    let system_time = humantime::parse_rfc3339(s)
        .map_err(|_| ParamError::Invalid { name, value: s.to_string() })?;
    let dur = system_time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ParamError::Invalid { name, value: s.to_string() })?;
    i64::try_from(dur.as_millis()).map_err(|_| ParamError::Invalid { name, value: s.to_string() })
}

/// Prometheus duration syntax (`30s`, `5m`) or bare float seconds.
pub fn parse_duration_ms(name: &'static str, s: &str) -> Result<i64, ParamError> {
    if let Ok(secs) = s.parse::<f64>() {
        return seconds_to_ms(name, s, secs);
    }
    let dur = humantime::parse_duration(s)
        .map_err(|_| ParamError::Invalid { name, value: s.to_string() })?;
    i64::try_from(dur.as_millis()).map_err(|_| ParamError::Invalid { name, value: s.to_string() })
}

fn seconds_to_ms(name: &'static str, raw: &str, secs: f64) -> Result<i64, ParamError> {
    if !secs.is_finite() {
        return Err(ParamError::Invalid { name, value: raw.to_string() });
    }
    let ms = secs * 1000.0;
    if ms < i64::MIN as f64 || ms > i64::MAX as f64 {
        return Err(ParamError::Invalid { name, value: raw.to_string() });
    }
    Ok(ms as i64)
}

pub fn decode_commit_tokens(raw: &[String]) -> Result<Vec<CommitToken>, ParamError> {
    raw.iter()
        .map(|s| {
            CommitToken::decode(s).map_err(|_| ParamError::Invalid {
                name: "min_commit_token",
                value: s.clone(),
            })
        })
        .collect()
}

pub fn parse_deadline(params: &Params, default: Duration) -> Result<Duration, ParamError> {
    match params.first("timeout") {
        Some(s) => {
            let ms = parse_duration_ms("timeout", s)?;
            if ms <= 0 {
                return Err(ParamError::Invalid { name: "timeout", value: s.to_string() });
            }
            Ok(Duration::from_millis(ms as u64))
        }
        None => Ok(default),
    }
}
