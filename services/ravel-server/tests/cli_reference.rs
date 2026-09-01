//! Drift check for `docs/reference/ravel-server-flags.md` (ADR-1040 decision
//! 4). The generated table is rendered from `Cli`'s clap definition and
//! compared to the committed file; `RAVEL_UPDATE_CLI_REFERENCE=1` rewrites the
//! file instead of asserting.

use std::env;
use std::path::{Path, PathBuf};

use clap::CommandFactory;
use ravel_server::Cli;
use ravel_server::cli_reference::{
    BEGIN_MARKER, END_MARKER, count_data_rows, flag_row, render_flags_block, splice, user_args,
};

/// Environment variable that switches the check into a rewrite, shared by both
/// crates' generators.
const UPDATE_ENV: &str = "RAVEL_UPDATE_CLI_REFERENCE";

/// The exact command a failing run prints so the fix is copy-pasteable.
const REGEN: &str =
    "RAVEL_UPDATE_CLI_REFERENCE=1 cargo test -p ravel-server --test cli_reference";

/// `docs/reference/ravel-server-flags.md`, resolved from this crate's manifest
/// directory rather than the process working directory (which differs between
/// a crate-scoped `cargo test` and one run from the workspace root).
fn reference_doc() -> PathBuf {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is always set");
    Path::new(&manifest)
        .join("..")
        .join("..")
        .join("docs")
        .join("reference")
        .join("ravel-server-flags.md")
}

/// The generated table has exactly one row per argument clap reports, counted
/// from the command definition rather than from the rendered text, so a
/// generator that emitted a single row of the ninety-odd would fail here.
#[test]
fn table_has_one_row_per_argument() {
    let cmd = Cli::command();
    let expected = user_args(&cmd).count();
    let block = render_flags_block(&cmd);
    let rows = count_data_rows(&block);
    assert!(
        expected > 50,
        "only {expected} arguments found on the ravel-server command, so the \
         definition did not load"
    );
    assert_eq!(
        rows, expected,
        "the generated ravel-server table has {rows} rows but the command \
         defines {expected} arguments"
    );
}

/// The environment-variable column is populated for a flag known to carry one
/// and empty for a flag known not to, so a generator that dropped the column
/// would fail rather than silently render blanks everywhere.
#[test]
fn environment_column_reflects_the_definition() {
    let cmd = Cli::command();

    let with_env = user_args(&cmd)
        .find(|arg| arg.get_long() == Some("s3-bucket"))
        .expect("--s3-bucket exists");
    assert_eq!(
        flag_row(with_env).env,
        "RAVEL_S3_BUCKET",
        "--s3-bucket must report its environment variable"
    );

    let without_env = user_args(&cmd)
        .find(|arg| arg.get_long() == Some("shards"))
        .expect("--shards exists");
    assert!(
        flag_row(without_env).env.is_empty(),
        "--shards has no environment variable and must render an empty cell"
    );
}

/// The committed reference matches what the current command definition
/// renders. Adding or changing a flag without regenerating fails here.
#[test]
fn cli_reference_is_current() {
    let cmd = Cli::command();
    let block = render_flags_block(&cmd);

    let path = reference_doc();
    let doc = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let updated = splice(&doc, &block)
        .unwrap_or_else(|| panic!("{} is missing the generated markers", path.display()));

    if env::var(UPDATE_ENV).as_deref() == Ok("1") {
        if updated != doc {
            std::fs::write(&path, &updated)
                .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
            eprintln!("rewrote {}", path.display());
        }
        return;
    }

    assert_eq!(
        updated, doc,
        "docs/reference/ravel-server-flags.md is stale. Regenerate it with:\n  \
         {REGEN}\nMarkers: {BEGIN_MARKER} .. {END_MARKER}"
    );
}
