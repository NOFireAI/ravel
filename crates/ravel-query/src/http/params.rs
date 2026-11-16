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
        self.values
            .get(key)
            .and_then(|v| v.first())
            .map(String::as_str)
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
    let system_time = humantime::parse_rfc3339(s).map_err(|_| ParamError::Invalid {
        name,
        value: s.to_string(),
    })?;
    let dur = system_time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ParamError::Invalid {
            name,
            value: s.to_string(),
        })?;
    i64::try_from(dur.as_millis()).map_err(|_| ParamError::Invalid {
        name,
        value: s.to_string(),
    })
}

/// Prometheus duration syntax (`30s`, `5m`) or bare float seconds.
pub fn parse_duration_ms(name: &'static str, s: &str) -> Result<i64, ParamError> {
    if let Ok(secs) = s.parse::<f64>() {
        return seconds_to_ms(name, s, secs);
    }
    let dur = humantime::parse_duration(s).map_err(|_| ParamError::Invalid {
        name,
        value: s.to_string(),
    })?;
    i64::try_from(dur.as_millis()).map_err(|_| ParamError::Invalid {
        name,
        value: s.to_string(),
    })
}

fn seconds_to_ms(name: &'static str, raw: &str, secs: f64) -> Result<i64, ParamError> {
    if !secs.is_finite() {
        return Err(ParamError::Invalid {
            name,
            value: raw.to_string(),
        });
    }
    let ms = secs * 1000.0;
    if ms < i64::MIN as f64 || ms > i64::MAX as f64 {
        return Err(ParamError::Invalid {
            name,
            value: raw.to_string(),
        });
    }
    Ok(ms as i64)
}

/// Whether the client opted into partial (degraded-coverage) results
/// (ADR-0071 "partial results are consent-gated and envelope-visible"
/// amendment, decision 1). Absent or any non-truthy value means not opted in
/// (the safe default: partial coverage then fails with a typed `unavailable`
/// error). Truthy values are `true` and `1`, case-insensitive, matching the
/// bool-like query parameters Prometheus and Grafana already pass. An
/// unrecognized value is treated as not opted in rather than a parse error, so
/// a client that fat-fingers the flag fails safe (503) instead of silently
/// receiving partial data.
pub fn parse_allow_partial(params: &Params) -> bool {
    match params.first("allow_partial") {
        Some(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1"),
        None => false,
    }
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

/// Resolves the per-query wall deadline from the client `timeout` parameter.
///
/// `max` is the configured server wall-deadline (`EngineConfig::deadline`)
/// and is a hard ceiling per the resource-limit contract
/// (docs/query-engine.md "Budgets": the `timeout` param can only *lower* the
/// deadline). When `timeout` is absent the server maximum applies. When it is
/// present the parsed value is clamped to `max`, so a client can shorten its
/// own deadline but never extend it past the server budget.
pub fn parse_deadline(params: &Params, max: Duration) -> Result<Duration, ParamError> {
    match params.first("timeout") {
        Some(s) => {
            let ms = parse_duration_ms("timeout", s)?;
            if ms <= 0 {
                return Err(ParamError::Invalid {
                    name: "timeout",
                    value: s.to_string(),
                });
            }
            Ok(Duration::from_millis(ms as u64).min(max))
        }
        None => Ok(max),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const SERVER_MAX: Duration = Duration::from_secs(30);

    fn params_with_timeout(timeout: &str) -> Params {
        Params::parse(Some(&format!("timeout={timeout}")), None)
    }

    /// A client `timeout` above the configured server wall-deadline is
    /// clamped to that maximum, never honored verbatim. The effective
    /// deadline is the observable: an over-max timeout yields the 30 s server
    /// budget, not the full 3600 s.
    #[test]
    fn timeout_above_server_max_is_clamped_to_max() {
        let params = params_with_timeout("3600");
        let deadline = parse_deadline(&params, SERVER_MAX).expect("valid timeout");
        assert_eq!(
            deadline, SERVER_MAX,
            "an over-default timeout must be clamped to the server maximum"
        );
    }

    #[test]
    fn timeout_below_server_max_is_honored() {
        let params = params_with_timeout("5");
        let deadline = parse_deadline(&params, SERVER_MAX).expect("valid timeout");
        assert_eq!(
            deadline,
            Duration::from_secs(5),
            "a client value under the server maximum must be honored exactly"
        );
    }

    #[test]
    fn timeout_equal_to_server_max_is_honored() {
        let params = params_with_timeout("30");
        let deadline = parse_deadline(&params, SERVER_MAX).expect("valid timeout");
        assert_eq!(deadline, SERVER_MAX);
    }

    #[test]
    fn absent_timeout_uses_server_max() {
        let params = Params::parse(None, None);
        let deadline = parse_deadline(&params, SERVER_MAX).expect("no timeout");
        assert_eq!(deadline, SERVER_MAX);
    }

    #[test]
    fn non_positive_timeout_is_rejected() {
        let params = params_with_timeout("0");
        assert!(parse_deadline(&params, SERVER_MAX).is_err());
    }

    #[test]
    fn allow_partial_absent_is_not_opted_in() {
        // The safe default: no `allow_partial` means partial coverage fails
        // with a typed 503, never silently returns partial data.
        assert!(!parse_allow_partial(&Params::parse(None, None)));
    }

    #[test]
    fn allow_partial_truthy_values_opt_in() {
        for raw in [
            "allow_partial=true",
            "allow_partial=1",
            "allow_partial=TRUE",
        ] {
            assert!(
                parse_allow_partial(&Params::parse(Some(raw), None)),
                "{raw} must opt in"
            );
        }
    }

    #[test]
    fn allow_partial_falsey_or_garbage_is_not_opted_in() {
        for raw in [
            "allow_partial=false",
            "allow_partial=0",
            "allow_partial=yes",
            "allow_partial=",
        ] {
            assert!(
                !parse_allow_partial(&Params::parse(Some(raw), None)),
                "{raw} must NOT opt in (fail safe)"
            );
        }
    }
}
