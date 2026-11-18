//! The fold race (README.md, "The forced races"): a `ravel-cli` fold whose
//! clock runs ahead of every other clock by less than the seal margin's
//! `clock_skew_allowance`, racing a server fold over a corrupt HEAD just
//! before an hour seals. The skewed fold already counts that hour as sealed
//! and the server does not, so they build different HEADs. The s3proxy holds
//! the server's HEAD PUT while the skewed fold's CAS lands, so when the held
//! PUT is released the store refuses it (`412`/`409`): its CAS was conditioned
//! on the corrupt HEAD's version, which the skewed fold has replaced.

/// How far ahead the skewed fold's clock runs: inside the 300-second
/// `clock_skew_allowance` the seal margin is sized for, so a clock this far
/// ahead seals nothing an acknowledged write can still land in.
pub const FOLD_SKEW_S: u64 = 240;
const _: () = assert!(FOLD_SKEW_S < crate::clock::CLOCK_SKEW_ALLOWANCE_S);

/// A bound used by the clock window (`clock::fold_race_seal`): the latest a
/// server fold may read its clock after the race starts and still, before the
/// seal, build a pre-seal HEAD the skewed fold can overtake.
pub const MAX_SERVER_DELAY_S: u64 = 40;

/// The fold the skewed process runs, as `catalog fold --json` prints it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CliFold {
    pub no_op: bool,
    pub rebuilt: bool,
    pub watermark_hour: Option<u64>,
}

/// One server's on-demand fold, as `POST /api/v1/admin/fold` answers it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ServerFold {
    pub status: String,
    /// `None` when the answer carries no report, which a fold that spent its
    /// CAS retries gives.
    pub put_requests: Option<u64>,
}

/// `catalog fold --json` output.
pub fn parse_cli_fold(stdout: &str) -> Option<CliFold> {
    let v: serde_json::Value = serde_json::from_str(stdout).ok()?;
    Some(CliFold {
        no_op: v.get("no_op")?.as_bool()?,
        rebuilt: v.get("rebuilt")?.as_bool()?,
        watermark_hour: v.get("watermark_hour").and_then(serde_json::Value::as_u64),
    })
}

/// The on-demand fold route's JSON answer.
pub fn parse_server_fold(body: &str) -> Option<ServerFold> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    Some(ServerFold {
        status: v.get("status")?.as_str()?.to_string(),
        put_requests: v.get("put_requests").and_then(serde_json::Value::as_u64),
    })
}

/// Whether a server fold lost its HEAD CAS to the skewed fold's HEAD, one
/// sealed hour ahead of its own.
///
/// The judgement reads the store, not the server's own answer. The skewed fold
/// rebuilt `ahead_hour` and its CAS landed first, so when the s3proxy releases
/// the held server HEAD PUT the store refuses it: `412 Precondition Failed` (or
/// `409`), because the version it was conditioned on, the corrupt HEAD's, is
/// gone. `head_cas_status` is that status, from the proxy's `held-result`. A
/// `200` means the held PUT won, which is never an overtaken fold; any other
/// status, or none held, is not either. The server's own answer may be
/// `lost_cas` (clean code) or `published` (the `lost-cas-proceeds` mutant,
/// which carries on after the refusal), and is kept only for the details.
pub fn overtaken(cli: &CliFold, head_cas_status: Option<u16>, ahead_hour: u64) -> bool {
    !cli.no_op
        && cli.rebuilt
        && cli.watermark_hour == Some(ahead_hour)
        && matches!(head_cas_status, Some(412) | Some(409))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(no_op: bool, rebuilt: bool, wm: Option<u64>) -> CliFold {
        CliFold {
            no_op,
            rebuilt,
            watermark_hour: wm,
        }
    }

    fn server(status: &str, puts: Option<u64>) -> ServerFold {
        ServerFold {
            status: status.to_string(),
            put_requests: puts,
        }
    }

    #[test]
    fn only_a_refused_head_cas_after_the_skewed_rebuild_is_overtaken() {
        let won = cli(false, true, Some(11));
        // A refused HEAD CAS (412 or 409) after the skewed rebuild: overtaken,
        // whatever the server's own answer was. This rules out the old rule,
        // which required the server to report "lost_cas": the mutant answers
        // "published" after the same 412, and that still counts as won.
        assert!(overtaken(&won, Some(412), 11));
        assert!(overtaken(&won, Some(409), 11));
        // A held PUT that succeeded (200) is a win, never overtaken. This rules
        // out a rule that counts any held PUT as won.
        assert!(!overtaken(&won, Some(200), 11));
        // No PUT was held, or the store answered something else.
        assert!(!overtaken(&won, None, 11));
        assert!(!overtaken(&won, Some(500), 11));
        // The skewed fold rebased onto another HEAD, so a server's or a
        // scheduled fold's CAS landed first: no fresh rebuild to overtake with.
        assert!(!overtaken(&cli(false, false, Some(11)), Some(412), 11));
        // The skewed fold published no hour ahead of the server.
        assert!(!overtaken(&cli(false, true, Some(10)), Some(412), 11));
        assert!(!overtaken(&cli(true, false, Some(11)), Some(412), 11));
    }

    #[test]
    fn the_reports_parse() {
        let out = r#"{"no_op": false, "rebuilt": true, "watermark_hour": 495011,
                      "put_requests": 7, "store": "s3"}"#;
        assert_eq!(parse_cli_fold(out), Some(cli(false, true, Some(495_011))));
        assert_eq!(
            parse_cli_fold(r#"{"no_op": true, "rebuilt": false, "watermark_hour": null}"#),
            Some(cli(true, false, None))
        );
        assert_eq!(parse_cli_fold("no_op: false\n"), None);
        assert_eq!(
            parse_server_fold(r#"{"status": "lost_cas", "put_requests": 4}"#),
            Some(server("lost_cas", Some(4)))
        );
        assert_eq!(
            parse_server_fold(r#"{"status": "lost_cas", "published": false}"#),
            Some(server("lost_cas", None))
        );
        assert_eq!(parse_server_fold(r#"{"published": true}"#), None);
    }
}
