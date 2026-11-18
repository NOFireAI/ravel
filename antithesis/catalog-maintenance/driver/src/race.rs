//! The fold race (README.md, "The forced races"): a `ravel-cli` fold whose
//! clock runs ahead of every other clock by less than the seal margin's
//! `clock_skew_allowance`, and the two `--mode all` servers' on-demand folds,
//! started together over a corrupt HEAD just before an hour seals. The
//! skewed fold already counts that hour as sealed and the servers do not, so
//! when the skewed fold's HEAD CAS lands first, each server fold loses its
//! CAS to a HEAD that differs from the one it built.

/// How far ahead the skewed fold's clock runs: inside the 300-second
/// `clock_skew_allowance` the seal margin is sized for, so a clock this far
/// ahead seals nothing an acknowledged write can still land in.
pub const FOLD_SKEW_S: u64 = 240;
const _: () = assert!(FOLD_SKEW_S < crate::clock::CLOCK_SKEW_ALLOWANCE_S);

/// The latest the server folds are posted after the skewed fold starts.
pub const MAX_SERVER_DELAY_S: f64 = 40.0;

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
/// The skewed fold published `ahead_hour` from a rebuild, so its CAS was the
/// one conditioned on the corrupt HEAD and it landed first: any CAS
/// conditioned on that HEAD and made later lost to it. A server fold that
/// reports `lost_cas` after a PUT read the corrupt HEAD, built a snapshot and
/// made such a CAS. A server fold that read a valid HEAD instead ends as a
/// no-op before any PUT, whatever it reports.
pub fn overtaken(cli: &CliFold, server: &ServerFold, ahead_hour: u64) -> bool {
    !cli.no_op
        && cli.rebuilt
        && cli.watermark_hour == Some(ahead_hour)
        && server.status == "lost_cas"
        && server.put_requests.is_none_or(|n| n > 0)
}

/// Seconds after the skewed fold starts to post the server folds. The
/// skewed fold's CAS lands about `cli_s` after its start, and a server fold
/// reads HEAD when posted and makes its CAS about `server_s` later; it loses
/// to the skewed fold when posted inside `(cli_s - server_s, cli_s)`. `frac`,
/// in `[0, 1]`, picks the point in that window, from the last attempt's
/// timings.
pub fn server_delay_s(cli_s: f64, server_s: f64, frac: f64) -> f64 {
    (cli_s - server_s * frac.clamp(0.0, 1.0)).clamp(0.0, MAX_SERVER_DELAY_S)
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
    fn only_a_cas_lost_to_the_skewed_rebuild_is_overtaken() {
        let won = cli(false, true, Some(11));
        assert!(overtaken(&won, &server("lost_cas", Some(3)), 11));
        // A server that spent its CAS retries lost them all.
        assert!(overtaken(&won, &server("lost_cas", None), 11));
        // No PUT: the server read a valid HEAD and stopped before any CAS.
        assert!(!overtaken(&won, &server("lost_cas", Some(0)), 11));
        // The server's own HEAD landed.
        assert!(!overtaken(&won, &server("published", Some(3)), 11));
        // The skewed fold rebased onto another HEAD, so a server's or a
        // scheduled fold's CAS landed first.
        assert!(!overtaken(
            &cli(false, false, Some(11)),
            &server("lost_cas", Some(3)),
            11
        ));
        // The skewed fold published no hour ahead of the servers.
        assert!(!overtaken(
            &cli(false, true, Some(10)),
            &server("lost_cas", Some(3)),
            11
        ));
        assert!(!overtaken(
            &cli(true, false, Some(11)),
            &server("lost_cas", Some(3)),
            11
        ));
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

    #[test]
    fn the_server_folds_are_posted_inside_the_losing_window() {
        // The skewed fold takes 6 s, a server fold 4 s: a post in (2, 6)
        // reads HEAD before the skewed CAS and makes its own after it.
        for frac in [0.1, 0.5, 0.9] {
            let d = server_delay_s(6.0, 4.0, frac);
            assert!(d > 2.0 && d < 6.0, "frac {frac}: {d}");
        }
        assert_eq!(server_delay_s(6.0, 4.0, 0.5), 4.0);
        // A server fold slower than the skewed one is posted at once.
        assert_eq!(server_delay_s(2.0, 9.0, 0.5), 0.0);
        // Never past the bound that keeps the servers before the seal.
        assert_eq!(server_delay_s(500.0, 1.0, 0.5), MAX_SERVER_DELAY_S);
        assert_eq!(server_delay_s(6.0, 4.0, 7.0), 2.0);
    }
}
