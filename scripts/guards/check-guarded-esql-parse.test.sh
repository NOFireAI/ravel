#!/usr/bin/env bash
# Cases for check-guarded-esql-parse.sh, written the way
# check-guarded-promql-parse.test.sh is written for the PromQL guard: a case
# before a rule change, never after.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout. Every fixture carries all three
# default roots: the crate and both callers.
#
# Two wrong implementations this file exists to rule out:
#   * a scan that finds nothing and reports clean (no entry, no sources, a
#     root that is not there): every such case asserts exit 2, never 0;
#   * a scan over crates/ravel-esql/src only, which passes every in-crate
#     case while a caller reaches the lexer around `lex`: every case under
#     "findings, caller roots" fails against it.
#
# Run: bash scripts/guards/check-guarded-esql-parse.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-guarded-esql-parse.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-guarded-esql-parse-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed, the two entries in
# the crate, and a caller that reaches them. Prints its path. Cases that need
# a broken anchor overwrite a file.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/crates/ravel-esql/src" \
    "${dir}/crates/ravel-sql/src/esql" "${dir}/services/ravel-server/src"
  cp "${GUARD}" "${dir}/scripts/guards/check-guarded-esql-parse.sh"
  cat >"${dir}/crates/ravel-esql/src/parser.rs" <<'RS'
//! The parser. Its prose may say pub fn parse_text(source: &str) freely.
pub fn parse(source: &str, limits: &Limits) -> Result<Query, Error> {
    let tokens = lex(source, limits)?;
    Parser { source, tokens }.query()
}

impl<'a> Parser<'a> {
    fn query(&mut self) -> Result<Query, Error> {
        self.pipeline()
    }

    pub(crate) fn text(&self, span: Span) -> &'a str {
        &self.source[span.start..span.end]
    }
}

fn unquote(text: &str) -> String {
    text.to_string()
}
RS
  cat >"${dir}/crates/ravel-esql/src/lexer.rs" <<'RS'
pub fn lex(source: &str, limits: &Limits) -> Result<Vec<Token>, Error> {
    let mut lexer = Lexer { src: source, pos: 0 };
    lexer.all(limits)
}

/// The earlier name of [`lex`].
pub use self::lex as tokenize;

pub(crate) fn is_word(text: &str) -> bool {
    !text.is_empty()
}
RS
  cat >"${dir}/crates/ravel-esql/src/ast.rs" <<'RS'
impl BinaryOp {
    pub fn symbol(self) -> &'static str {
        "+"
    }

    pub fn describe(&self) -> String {
        format!("pub fn parse_from(text: &str)")
    }
}
RS
  cat >"${dir}/crates/ravel-sql/src/esql/mod.rs" <<'RS'
use ravel_esql::{Limits, Token};
use sqlparser::parser::Parser;

pub fn bind(source: &str, limits: &Limits) -> Result<Plan, EsqlError> {
    let query = ravel_esql::parse(source, limits).map_err(EsqlError::Parse)?;
    let tokens: Vec<Token> = ravel_esql::lexer::tokenize(source, limits)?;
    let sql = Parser::new(&dialect);
    Ok(plan(query, tokens.len()))
}
RS
  cat >"${dir}/services/ravel-server/src/main.rs" <<'RS'
fn main() {
    serve();
}
RS
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-guarded-esql-parse.sh 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- the clean shape -------------------------------------------------------

d="$(new_repo clean)"
check "passes when only parse and lex take source text" "${d}" 0 \
  "only parse and lex take ES|QL source text"

d="$(new_repo clean_callers)"
cat >"${d}/services/ravel-server/src/redact.rs" <<'RS'
use ravel_esql::ast::*;
use ravel_esql::{
    Span,
    lex,
};
use sqlparser::parser;

pub fn redact(source: &str, limits: &ravel_esql::Limits) -> String {
    let tokens = ravel_esql::lexer::lex(source, limits).unwrap_or_default();
    let ast = parser::Parser::parse_sql(&dialect, source);
    let kind = ravel_esql::TokenKind::Pipe;
    mask(tokens, kind)
}
RS
check "a caller may reach lex, root re-exports and another crate's parser" "${d}" 0 ""

# --- findings, the crate ---------------------------------------------------

d="$(new_repo second_entry)"
cat >>"${d}/crates/ravel-esql/src/parser.rs" <<'RS'

pub fn parse_pipeline(source: &str) -> Result<Vec<Command>, Error> {
    Parser::unlimited(source).pipeline()
}
RS
check "flags a second public function taking source text" "${d}" 1 \
  "parser.rs:21: source-entry:"

d="$(new_repo private_module)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn check_text(text: &str) -> Result<(), Error> {
    Ok(())
}
RS
check "flags a bare pub inside a private module" "${d}" 1 "guard.rs:1: source-entry:"

d="$(new_repo multiline_sig)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn check(
    limits: &Limits,
    text: &str,
) -> Result<(), Error> {
    Ok(())
}
RS
check "flags a source parameter on a later line of the signature" "${d}" 1 \
  "guard.rs:1: source-entry:"

d="$(new_repo generic_param)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn parse_with<S: AsRef<str>>(source: S, limits: &Limits) -> Query {
    todo()
}
RS
check "flags source text taken through a generic bound" "${d}" 1 "guard.rs:1: source-entry:"

d="$(new_repo where_clause)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn lex_from<R>(reader: R) -> Vec<Token>
where
    R: Read,
{
    todo()
}
RS
check "flags source text taken through a where clause" "${d}" 1 "guard.rs:1: source-entry:"

d="$(new_repo byte_slice)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub const fn first_byte(bytes: &[u8]) -> u8 {
    bytes[0]
}
RS
check "flags a byte slice parameter on a pub const fn" "${d}" 1 "guard.rs:1: source-entry:"

d="$(new_repo method)"
cat >>"${d}/crates/ravel-esql/src/ast.rs" <<'RS'

impl Query {
    pub fn from_text(text: String) -> Self {
        todo()
    }
}
RS
check "flags a public method taking source text" "${d}" 1 "ast.rs:12: source-entry:"

d="$(new_repo trait_method)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub trait Source {
    fn parse_text(&self, text: &str) -> Query;
}

trait Private {
    fn parse_text(&self, text: &str) -> Query;
}
RS
check "flags a public trait method taking source text" "${d}" 1 "guard.rs:2: source-entry:"

d="$(new_repo from_str)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
impl std::str::FromStr for Query {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        todo()
    }
}
RS
check "flags an impl of FromStr" "${d}" 1 "guard.rs:1: source-entry:"

d="$(new_repo from_ref)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
impl From<Span> for Error {
    fn from(span: Span) -> Self {
        todo()
    }
}

impl<'a> TryFrom<&'a str> for Query {
    type Error = Error;
    fn try_from(s: &'a str) -> Result<Self, Error> {
        todo()
    }
}
RS
check "flags TryFrom over source text, not From over another type" "${d}" 1 \
  "guard.rs:7: source-entry:"

d="$(new_repo redefined_alias)"
cat >"${d}/crates/ravel-esql/src/lexer.rs" <<'RS'
pub fn lex(source: &str, limits: &Limits) -> Result<Vec<Token>, Error> {
    tokenize(source)
}

pub fn tokenize(source: &str) -> Result<Vec<Token>, Error> {
    Lexer::new(source).all()
}
RS
check "flags an old entry name defined as its own function" "${d}" 1 \
  "lexer.rs:5: source-entry:"

# Tokens are lexed source: a public way to parse them skips guard::check's
# nesting and stage limits even when `lex` produced them.
d="$(new_repo token_slice)"
cat >>"${d}/crates/ravel-esql/src/parser.rs" <<'RS'

pub fn parse_tokens(tokens: &[Token], limits: &Limits) -> Result<Query, Error> {
    Parser { tokens }.query()
}
RS
check "flags a public function taking a slice of tokens" "${d}" 1 \
  "parser.rs:21: source-entry: public \`parse_tokens\` takes lexed tokens"

d="$(new_repo token_vec)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
impl Query {
    pub fn from_tokens(
        tokens: Vec< Token >,
    ) -> Result<Self, Error> {
        todo()
    }
}
RS
check "flags a public method taking a vector of tokens" "${d}" 1 \
  "guard.rs:2: source-entry: public \`from_tokens\` takes lexed tokens"

d="$(new_repo token_iter)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn parse_iter<I: IntoIterator<Item = Token>>(tokens: I) -> Result<Query, Error> {
    todo()
}
RS
check "flags a public function taking an iterator of tokens" "${d}" 1 \
  "guard.rs:1: source-entry: public \`parse_iter\` takes lexed tokens"

d="$(new_repo token_crate_path)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn parse_tokens(tokens: &[crate::Token]) -> Result<Query, Error> {
    todo()
}
RS
check "flags a slice of tokens named through the crate root" "${d}" 1 \
  "guard.rs:1: source-entry: public \`parse_tokens\` takes lexed tokens"

d="$(new_repo token_lexer_path)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn parse_tokens(tokens: &[crate::lexer::Token]) -> Result<Query, Error> {
    todo()
}
RS
check "flags a slice of tokens named through the lexer module" "${d}" 1 \
  "guard.rs:1: source-entry: public \`parse_tokens\` takes lexed tokens"

d="$(new_repo token_vec_path)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn parse_tokens(tokens: Vec<lexer::Token>) -> Result<Query, Error> {
    todo()
}
RS
check "flags a vector of tokens named through a module" "${d}" 1 \
  "guard.rs:1: source-entry: public \`parse_tokens\` takes lexed tokens"

d="$(new_repo token_iter_ref)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub fn parse_iter<'a, I: Iterator<Item = &'a Token>>(tokens: I) -> Result<Query, Error> {
    todo()
}
RS
check "flags an iterator of token references" "${d}" 1 \
  "guard.rs:1: source-entry: public \`parse_iter\` takes lexed tokens"

d="$(new_repo token_from)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
impl From<Vec<Token>> for Query {
    fn from(tokens: Vec<Token>) -> Self {
        todo()
    }
}
RS
check "flags From over a vector of tokens" "${d}" 1 "guard.rs:1: source-entry:"

d="$(new_repo token_internal)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
pub(crate) fn check(tokens: &[Token], limits: &Limits) -> Result<(), Error> {
    Ok(())
}

impl Query {
    pub fn tokens(&self) -> &[Token] {
        &self.tokens
    }

    pub fn describe(kind: &TokenKind, all: &[TokenKind]) -> String {
        String::new()
    }
}
RS
check "passes crate-private token use, returned tokens and token kinds" "${d}" 0 ""

# --- findings, caller roots ------------------------------------------------
#
# The cases a crate-only scan cannot fail: the crate is clean, and a caller
# reaches the lexer or parser around the guarded entries.

d="$(new_repo caller_qualified)"
cat >>"${d}/crates/ravel-sql/src/esql/mod.rs" <<'RS'

fn raw(source: &str) -> Vec<Token> {
    ravel_esql::lexer::Lexer::new(source).collect()
}
RS
check "flags a qualified path to a lexer item" "${d}" 1 "mod.rs:12: unguarded-reference:"

d="$(new_repo caller_module_use)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
use ravel_esql::lexer;
RS
check "flags a use of the lexer module" "${d}" 1 "raw.rs:1: unguarded-reference:"

d="$(new_repo caller_braced_use)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
use ravel_esql::{Span, parser::Parser};
RS
check "flags a parser item inside braces" "${d}" 1 "raw.rs:1: unguarded-reference:"

d="$(new_repo caller_multiline_use)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
use ravel_esql::{
    Span,
    lexer::{self, Token},
};
RS
check "flags a lexer item in a multi-line use" "${d}" 1 "raw.rs:3: unguarded-reference:"

d="$(new_repo caller_crate_alias)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
use ravel_esql as esql;

fn raw(source: &str) -> usize {
    esql::lexer::Lexer::new(source).count()
}
RS
check "flags a lexer path through a crate alias" "${d}" 1 "raw.rs:4: unguarded-reference:"

d="$(new_repo caller_glob)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
use ravel_esql::*;

fn raw(source: &str) -> usize {
    lexer::Lexer::new(source).count()
}
RS
check "flags a bare lexer path after a glob import of the crate" "${d}" 1 \
  "raw.rs:4: unguarded-reference:"

d="$(new_repo caller_past_entry)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
fn raw(source: &str) -> usize {
    ravel_esql::lexer::lex::inner(source)
}
RS
check "flags a path that continues past an entry" "${d}" 1 "raw.rs:2: unguarded-reference:"

d="$(new_repo caller_server)"
cat >"${d}/services/ravel-server/src/main.rs" <<'RS'
fn main() {
    let tokens = ravel_esql::lexer::Lexer::new(QUERY);
}
RS
check "flags a lexer path under the second caller root" "${d}" 1 \
  "main.rs:2: unguarded-reference:"

# --- prose and strings are not code ----------------------------------------

d="$(new_repo prose)"
cat >"${d}/crates/ravel-sql/src/esql/notes.rs" <<'RS'
//! ravel_esql::lexer::Lexer is private; ravel_esql::parser::Parser too.
/* use ravel_esql::lexer; in a block comment,
   spanning two lines: ravel_esql::lexer::Mode */
fn describe() -> &'static str {
    "ravel_esql::lexer::Lexer"
}
RS
check "ignores references in comments and string literals" "${d}" 0 ""

# --- the allow marker ------------------------------------------------------

d="$(new_repo marker_inline)"
cat >"${d}/crates/ravel-esql/src/span.rs" <<'RS'
impl Span {
    pub fn slice<'a>(&self, source: &'a str) -> &'a str { // guarded-esql-parse-allow: indexes text
        &source[self.start..self.end]
    }
}
RS
check "an inline marker with a reason suppresses" "${d}" 0 ""

d="$(new_repo marker_block)"
cat >"${d}/crates/ravel-esql/src/span.rs" <<'RS'
impl Span {
    /// The line and column of the span's start.
    // guarded-esql-parse-allow: counts lines and characters; reads no syntax.
    pub fn line_col(&self, source: &str) -> LineCol {
        LineCol::at(source, self.start)
    }
}
RS
check "a marker in the block directly above the line suppresses" "${d}" 0 ""

d="$(new_repo marker_no_reason)"
cat >"${d}/crates/ravel-esql/src/span.rs" <<'RS'
impl Span {
    // guarded-esql-parse-allow:
    pub fn line_col(&self, source: &str) -> LineCol {
        LineCol::at(source, self.start)
    }
}
RS
check "a marker with no reason does not suppress" "${d}" 1 "span.rs:3: source-entry:"

d="$(new_repo marker_stale)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
// guarded-esql-parse-allow: this reason belongs to the function below it.
fn documented() {}

fn later(source: &str) -> usize {
    ravel_esql::lexer::Lexer::new(source).count()
}
RS
check "a marker does not carry past its own comment block" "${d}" 1 \
  "raw.rs:5: unguarded-reference:"

d="$(new_repo marker_in_string)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
fn later(source: &str) -> usize {
    ravel_esql::lexer::Lexer::new(source).expect("guarded-esql-parse-allow: not a comment")
}
RS
check "the marker only counts from a comment, not a string" "${d}" 1 \
  "raw.rs:2: unguarded-reference:"

# The SQL guard's marker answers a different question; it must not silence
# this guard, in either root, inline or in the block above.
d="$(new_repo marker_sql_crate)"
cat >"${d}/crates/ravel-esql/src/span.rs" <<'RS'
impl Span {
    // guarded-parse-allow: counts lines and characters; reads no syntax.
    pub fn line_col(&self, source: &str) -> LineCol {
        LineCol::at(source, self.start)
    }

    pub fn slice<'a>(&self, source: &'a str) -> &'a str { // guarded-parse-allow: indexes text
        &source[self.start..self.end]
    }
}
RS
check "the SQL guard's marker does not suppress in the crate" "${d}" 1 \
  "span.rs:3: source-entry:"
check "the SQL guard's inline marker does not suppress in the crate" "${d}" 1 \
  "span.rs:7: source-entry:"

d="$(new_repo marker_sql_caller)"
cat >"${d}/crates/ravel-sql/src/esql/raw.rs" <<'RS'
fn later(source: &str) -> usize {
    // guarded-parse-allow: parses fixture text.
    ravel_esql::lexer::Lexer::new(source).count()
}
RS
check "the SQL guard's marker does not suppress in a caller" "${d}" 1 \
  "raw.rs:3: unguarded-reference:"

d="$(new_repo marker_tokens)"
cat >"${d}/crates/ravel-esql/src/guard.rs" <<'RS'
// guarded-esql-parse-allow: replays tokens a parse already checked.
pub fn replay(tokens: &[Token]) -> usize {
    tokens.len()
}
RS
check "this guard's marker suppresses a token finding" "${d}" 0 ""

# --- anchors ---------------------------------------------------------------

d="$(new_repo parse_renamed)"
cat >"${d}/crates/ravel-esql/src/parser.rs" <<'RS'
pub fn parse_query(source: &str, limits: &Limits) -> Result<Query, Error> {
    todo()
}
RS
check "refuses to run when parse is renamed" "${d}" 2 "expected exactly one"

d="$(new_repo lex_renamed)"
cat >"${d}/crates/ravel-esql/src/lexer.rs" <<'RS'
pub fn lex_all(source: &str, limits: &Limits) -> Result<Vec<Token>, Error> {
    todo()
}
RS
check "refuses to run when lex is renamed" "${d}" 2 "expected exactly one"

d="$(new_repo parse_duplicated)"
cat >>"${d}/crates/ravel-esql/src/parser.rs" <<'RS'

pub fn parse(source: &str) -> Result<Query, Error> {
    todo()
}
RS
check "refuses when a second parse appears in the anchor file" "${d}" 2 "expected exactly one"

d="$(new_repo parse_untyped)"
cat >"${d}/crates/ravel-esql/src/parser.rs" <<'RS'
pub fn parse(source: &Statement, limits: &Limits) -> Result<Query, Error> {
    todo()
}
RS
check "refuses when parse no longer reads as taking source text" "${d}" 2 \
  "no longer reads as taking source text"

d="$(new_repo callers_unanchored)"
cat >"${d}/crates/ravel-sql/src/esql/mod.rs" <<'RS'
pub fn bind(query: &Query) -> Result<Plan, EsqlError> {
    Ok(plan(query))
}
RS
check "refuses when no caller names a guarded entry" "${d}" 2 \
  "name neither ravel_esql::parse nor"

# --- a scan that would find nothing ----------------------------------------

d="$(new_repo empty_roots)"
rm -f "${d}/crates/ravel-sql/src/esql/mod.rs" "${d}/services/ravel-server/src/main.rs"
check "refuses when the caller roots hold no Rust sources" "${d}" 2 "no Rust sources"

d="$(new_repo missing_default_root)"
rm -rf "${d}/services/ravel-server"
check "refuses when a default root is missing" "${d}" 2 "no such directory"

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
out="$(cd "${d}" && bash scripts/guards/check-guarded-esql-parse.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    %s\n' "an unknown option is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "an unknown option is a usage error" "${rc}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-guarded-esql-parse.sh \
  crates/ravel-esql/src crates/nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no such directory"* ]]; then
  printf 'ok    %s\n' "a named root that is not there is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "a named root that is not there is a usage error" "${rc}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-guarded-esql-parse.sh \
  crates/ravel-esql/src 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"two roots"* ]]; then
  printf 'ok    %s\n' "scanning the crate alone is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "scanning the crate alone is a usage error" "${rc}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
