//! Render the command-line reference tables for `ravel-server` and
//! `ravel-cli` from their `clap::Command` definitions (ADR-1040 decision 4).
//!
//! This module is the single source of the table format shared by both
//! crates' `cli_reference_is_current` tests: `ravel-server` renders its one
//! flat table with [`render_flags_block`], and `ravel-cli` walks its
//! subcommand tree with [`render_command_tree_block`]. Both use [`splice`] to
//! replace only the content between the generated markers, leaving each page's
//! hand-written preamble untouched.
//!
//! The tables are rendered from the command definition, never from a built
//! binary's `--help`: help output wraps to terminal width, which would make
//! the comparison non-deterministic.

use std::fmt::Write as _;

use clap::{Arg, Command};

/// Opening marker for the generated block. The generator rewrites everything
/// between this and [`END_MARKER`]; the preamble above stays hand-written.
pub const BEGIN_MARKER: &str = "<!-- BEGIN GENERATED FLAGS -->";
/// Closing marker for the generated block.
pub const END_MARKER: &str = "<!-- END GENERATED FLAGS -->";

/// One rendered row of a flag table.
pub struct FlagRow {
    /// The flag column: `--long` for an option, `<VALUE>` for a positional.
    pub flag: String,
    /// The `RAVEL_*` environment variable, empty when the flag has none.
    pub env: String,
    /// The default value clap reports, empty when there is none.
    pub default: String,
    /// The first line of the flag's help text.
    pub help: String,
}

/// Whether an argument is one the binary defines, as opposed to clap's
/// auto-generated `--help`/`--version`, which are not part of the reference.
fn is_user_arg(arg: &Arg) -> bool {
    let id = arg.get_id().as_str();
    id != "help" && id != "version"
}

/// The user-defined arguments of a single command, excluding clap's
/// auto-generated `help`/`version`. Both the renderer and the tests count
/// through this, so a row count derived from it is the argument count from the
/// command definition rather than a re-count of rendered text.
pub fn user_args(cmd: &Command) -> impl Iterator<Item = &Arg> {
    cmd.get_arguments().filter(|arg| is_user_arg(arg))
}

/// The flag column for one argument.
fn flag_name(arg: &Arg) -> String {
    if let Some(long) = arg.get_long() {
        format!("--{long}")
    } else {
        let name = arg
            .get_value_names()
            .and_then(|names| names.first())
            .map(|name| name.to_string())
            .unwrap_or_else(|| arg.get_id().as_str().to_uppercase());
        format!("<{name}>")
    }
}

/// The first line of a possibly multi-line string.
fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or("")
}

/// Escape a table cell so a `|` or newline in help text cannot split the row.
fn escape_cell(cell: &str) -> String {
    cell.replace('\\', "\\\\")
        .replace('\n', " ")
        .replace('|', "\\|")
}

/// Build the row for one argument from its clap accessors.
pub fn flag_row(arg: &Arg) -> FlagRow {
    let env = arg
        .get_env()
        .map(|env| env.to_string_lossy().into_owned())
        .unwrap_or_default();
    let default = arg
        .get_default_values()
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(", ");
    let help = arg
        .get_help()
        .map(|help| help.to_string())
        .unwrap_or_default();
    FlagRow {
        flag: flag_name(arg),
        env,
        default,
        help: first_line(&help).to_string(),
    }
}

/// A monospace cell, or an empty cell when the value is empty.
fn code_cell(value: &str) -> String {
    if value.is_empty() {
        String::new()
    } else {
        format!("`{}`", escape_cell(value))
    }
}

/// Render a set of arguments as one markdown table sorted by flag name. Every
/// data row's flag cell opens with a backtick, which the tests rely on to
/// count rows unambiguously against the header and separator lines.
fn render_table(args: &[&Arg]) -> String {
    let mut rows: Vec<FlagRow> = args.iter().map(|arg| flag_row(arg)).collect();
    rows.sort_by(|a, b| a.flag.cmp(&b.flag));

    let mut out = String::new();
    out.push_str("| Flag | Environment variable | Default | Help |\n");
    out.push_str("| --- | --- | --- | --- |\n");
    for row in &rows {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            code_cell(&row.flag),
            code_cell(&row.env),
            code_cell(&row.default),
            escape_cell(&row.help),
        );
    }
    out
}

/// Render `ravel-server`'s single flat table: one row per argument, sorted by
/// flag name. The returned block ends with a newline and is meant to sit
/// between [`BEGIN_MARKER`] and [`END_MARKER`].
pub fn render_flags_block(cmd: &Command) -> String {
    let args: Vec<&Arg> = user_args(cmd).collect();
    render_table(&args)
}

/// Render `ravel-cli`'s subcommand-structured reference: a table for the global
/// flags, then one heading and table per subcommand, walking the tree rather
/// than naming the subcommands. The returned block ends with a newline.
pub fn render_command_tree_block(cmd: &Command) -> String {
    let mut out = String::new();

    out.push_str("## Global flags\n\n");
    let global: Vec<&Arg> = user_args(cmd).collect();
    push_command_table(&mut out, &global);

    walk_subcommands(cmd, &[], &mut out);
    out
}

/// Append a command's table, or a placeholder line when it has no flags, so a
/// subcommand that only groups further subcommands still appears.
fn push_command_table(out: &mut String, args: &[&Arg]) {
    if args.is_empty() {
        out.push_str("_No flags._\n\n");
    } else {
        out.push_str(&render_table(args));
        out.push('\n');
    }
}

/// Depth-first walk of the subcommand tree, emitting one heading and table per
/// subcommand. `path` is the chain of subcommand names above `cmd`.
fn walk_subcommands(cmd: &Command, path: &[String], out: &mut String) {
    for sub in cmd.get_subcommands() {
        // clap's generated `help` subcommand is not part of the reference.
        if sub.get_name() == "help" {
            continue;
        }
        let mut sub_path = path.to_vec();
        sub_path.push(sub.get_name().to_string());

        let level = "#".repeat((sub_path.len() + 1).min(6));
        let _ = writeln!(out, "{level} {}\n", sub_path.join(" "));
        if let Some(about) = sub.get_about() {
            let about = about.to_string();
            let first = first_line(&about);
            if !first.is_empty() {
                let _ = writeln!(out, "{first}\n");
            }
        }

        let args: Vec<&Arg> = user_args(sub).collect();
        push_command_table(out, &args);

        walk_subcommands(sub, &sub_path, out);
    }
}

/// Replace the content between [`BEGIN_MARKER`] and [`END_MARKER`] in `doc`
/// with `block`, leaving the hand-written preamble and everything after the
/// end marker untouched. Returns `None` when either marker is absent.
pub fn splice(doc: &str, block: &str) -> Option<String> {
    let begin = doc.find(BEGIN_MARKER)?;
    let after_begin = begin + BEGIN_MARKER.len();
    let end = doc[after_begin..].find(END_MARKER)? + after_begin;

    let mut out = String::with_capacity(doc.len() + block.len());
    out.push_str(&doc[..after_begin]);
    out.push('\n');
    out.push_str(block);
    out.push_str(&doc[end..]);
    Some(out)
}

/// Count the data rows across every table in a rendered block. A data row is
/// the only line whose flag cell opens with a backtick, so this never counts a
/// header or separator line.
pub fn count_data_rows(block: &str) -> usize {
    block.lines().filter(|line| line.starts_with("| `")).count()
}
