#!/usr/bin/env bash
# Guarded-ES|QL-parse guard (issue #2892).
#
# `ravel_esql::parse` and `ravel_esql::lex` are the only entries that take
# ES|QL source text, and both refuse a statement over its byte limit before
# lexing and count tokens while lexing; `parse` also checks nesting and
# pipeline stages before the recursive parser runs. Any other public way in
# skips those limits, and the parser's recursion is what the limits bound.
# This guard refuses such a way in, so a new entry point fails the gate
# instead of resting on its author knowing the rule.
#
# The FIRST root is the crate itself (default crates/ravel-esql/src). There it
# refuses, outside comments and string literals:
#   * a function declared with a bare `pub` (not `pub(crate)` or
#     `pub(super)`) whose generics, parameters or where clause name `str`,
#     `String`, `[u8]`, `Read` or `BufRead`, other than `pub fn parse` in
#     parser.rs and `pub fn lex` in lexer.rs. Visibility is read from the
#     declaration alone: a bare `pub` inside a private module counts, because
#     one `pub use` exports it. Internal helpers say `pub(crate)`;
#   * the same for lexed tokens, `[Token]`, `Vec<Token>` or `Item = Token`:
#     tokens from `lex` have not been through the nesting and stage checks
#     that `parse` runs before the recursive parser;
#   * a function inside a `pub trait` body taking any of those;
#   * an `impl` of `FromStr`, or of `From`/`TryFrom` over any of those.
# The return type is not read: `fn symbol(self) -> &'static str` takes no
# text.
#
# The OTHER roots are the crate's callers (default crates/ravel-sql/src and
# services/ravel-server/src). There it refuses any path through the crate's
# `lexer` or `parser` module that does not end at `parse`, `lex`, or a name
# the crate declares as an alias of one of them (`pub use self::lex as
# tokenize` makes `ravel_esql::lexer::tokenize` the same item as `lex`). That
# covers a qualified path, a `use` of the module or of an item in it,
# including inside a braced or multi-line `use`, a path through a crate alias
# (`use ravel_esql as esql`), and a bare `lexer::`/`parser::` path in a file
# that glob-imports the crate. Root re-exports such as `ravel_esql::Token`
# are not refused: the first root's rules already keep every public item
# other than the two entries from taking source text.
#
# ANCHORS, so a rename cannot turn the scan into a no-op that passes
# everything: `pub fn parse` exactly once in crates/ravel-esql/src/parser.rs
# and `pub fn lex` exactly once in crates/ravel-esql/src/lexer.rs, each
# read by the rule above as taking source text (if not, the rule has drifted
# from the code it guards); and at least one reference to a guarded entry
# under the caller roots (if none, the callers reach the crate through
# something this guard does not read, or not at all, and the roots need
# re-pointing).
#
# Usage:
#   scripts/guards/check-guarded-esql-parse.sh [crate-root caller-root ...]
#     default: crates/ravel-esql/src crates/ravel-sql/src services/ravel-server/src
#
# Exit 0 clean, 1 on findings, 2 when the scan would be a no-op (an anchor is
# gone, a default root is missing, or the roots hold no Rust sources), 64 on
# bad usage (an unknown option, a named root that is not there, fewer than two
# roots). Findings print as `file:line: <rule>: explanation`, where rule is
# `source-entry` (first root) or `unguarded-reference` (caller roots).
#
# Escape hatch, per finding, on the flagged line's own comment or anywhere in
# the contiguous comment block immediately above it:
#
#   // guarded-esql-parse-allow: <reason>
#
# The reason is required: the marker without one does not suppress. A public
# helper that maps spans to positions in text already parsed is what it is
# for. A new parse or lex entry is not: route it through `parse` or `lex`.
# The marker is this guard's own: check-guarded-sql-parse.sh's
# `guarded-parse-allow:` does not suppress a finding here.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

anchor_parse_file="crates/ravel-esql/src/parser.rs"
anchor_lex_file="crates/ravel-esql/src/lexer.rs"

default_roots=(crates/ravel-esql/src crates/ravel-sql/src services/ravel-server/src)

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,68p' "$0"
      exit 0
      ;;
    -*)
      echo "check-guarded-esql-parse.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done

roots_from_cli=1
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=("${default_roots[@]}")
  roots_from_cli=0
fi

if [[ ${#roots[@]} -lt 2 ]]; then
  echo "check-guarded-esql-parse.sh: needs at least two roots, the crate and" \
    "a caller; the callers are where a second way in would be used, and a" \
    "scan of the crate alone passes it silently" >&2
  exit 64
fi

missing_rc=2
if [[ ${roots_from_cli} -eq 1 ]]; then
  missing_rc=64
fi
for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-guarded-esql-parse.sh: no such directory: ${root}" >&2
    exit "${missing_rc}"
  fi
done

crate_root="${roots[0]%/}/"

# The crate's files go first so its aliases of the entries are known before
# any caller line is read.
crate_sources=()
while IFS= read -r -d '' file; do
  crate_sources+=("${file}")
done < <(find "${roots[0]}" -type f -name '*.rs' -not -path '*/target/*' -print0 | sort -z)

caller_sources=()
while IFS= read -r -d '' file; do
  caller_sources+=("${file}")
done < <(find "${roots[@]:1}" -type f -name '*.rs' -not -path '*/target/*' -print0 | sort -z)

if [[ ${#crate_sources[@]} -eq 0 || ${#caller_sources[@]} -eq 0 ]]; then
  echo "check-guarded-esql-parse.sh: no Rust sources under ${roots[0]} or" \
    "under ${roots[*]:1}; a scan over zero files reports clean, which is the" \
    "state this guard refuses rather than reports" >&2
  exit 2
fi

# The comment and string stripper is check-guarded-promql-parse.sh's: state
# carried across lines, char literals told apart from lifetimes.
awk_prog='
function charlit_len(s, i,   c2, c, j) {
  c2 = substr(s, i + 1, 1)
  if (c2 == "\\") {
    j = i + 2
    c = substr(s, j, 1)
    if (c == "x") j = j + 3
    else if (c == "u") {
      j = j + 1
      while (j <= length(s) && substr(s, j, 1) != "}") j++
      j++
    } else j = j + 1
    if (substr(s, j, 1) == SQ) return j - i + 1
    return 0
  }
  if (substr(s, i + 2, 1) == SQ) return 3
  return 0
}
function strip_line(s,   out, i, n, c, j, k, hashes, hs, cl) {
  out = ""; cmt = ""; n = length(s); i = 1
  while (i <= n) {
    c = substr(s, i, 1)
    if (bdepth > 0) {
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; i += 2; continue }
      cmt = cmt c; i++; continue
    }
    if (sstate == 1) {
      if (sesc) { sesc = 0; i++; continue }
      if (c == "\\") { sesc = 1; i++; continue }
      if (c == "\"") sstate = 0
      i++; continue
    }
    if (sstate == 2) {
      if (c == "\"") {
        hs = ""
        for (j = 0; j < shashes; j++) hs = hs "#"
        if (shashes == 0 || substr(s, i + 1, shashes) == hs) {
          sstate = 0; i += 1 + shashes; continue
        }
      }
      i++; continue
    }
    if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth = 1; i += 2; continue }
    if (c == "/" && substr(s, i + 1, 1) == "/") { cmt = cmt substr(s, i); break }
    if (c == SQ) {
      cl = charlit_len(s, i)
      if (cl > 0) { i += cl; continue }
      out = out c; i++; continue
    }
    if (c == "r" || (c == "b" && substr(s, i + 1, 1) == "r")) {
      j = i
      if (c == "b") j++
      k = j + 1
      hashes = 0
      while (substr(s, k, 1) == "#") { hashes++; k++ }
      if (substr(s, k, 1) == "\"") { sstate = 2; shashes = hashes; i = k + 1; continue }
    }
    if (c == "\"") { sstate = 1; i++; continue }
    out = out c; i++
  }
  return out
}
function has_marker(s,   i, rest) {
  i = index(s, MARKER)
  if (i == 0) return 0
  rest = substr(s, i + length(MARKER))
  gsub(/[ \t]/, "", rest)
  return length(rest) > 0
}
function takes_text(s) {
  return s ~ /(^|[^A-Za-z0-9_])(str|String|Read|BufRead)([^A-Za-z0-9_]|$)/ ||
         s ~ /\[[ \t]*u8[ \t]*\]/
}
# A path prefix (`crate::lexer::`), a reference and a lifetime are dropped
# first, so `&[crate::Token]` reads as `[Token]` and an iterator over token
# references with a named lifetime reads as `Item = Token`.
function takes_tokens(s,   t) {
  t = s
  gsub(/[A-Za-z_][A-Za-z0-9_]*[ \t]*::[ \t]*/, "", t)
  gsub("\047[A-Za-z_][A-Za-z0-9_]*", "", t)
  gsub(/&[ \t]*(mut[ \t]+)?/, "", t)
  return t ~ /\[[ \t]*Token[ \t]*\]/ ||
         t ~ /(^|[^A-Za-z0-9_])Vec[ \t]*<[ \t]*Token[ \t]*>/ ||
         t ~ /(^|[^A-Za-z0-9_])Item[ \t]*=[ \t]*Token([^A-Za-z0-9_]|$)/
}
function takes_input(s) {
  return takes_text(s) || takes_tokens(s)
}
# The type argument of a `From<...>` or `TryFrom<...>` in an impl header, up
# to its matching `>`, so `From<Vec<Token>>` keeps its inner type and
# `for Wrapper<String>` stays out of it.
function from_arg(header,   at, i, n, c, open) {
  if (!match(header, /(^|[^A-Za-z0-9_])(Try)?From[ \t]*</)) return ""
  at = RSTART + RLENGTH
  n = length(header)
  open = 1
  for (i = at; i <= n; i++) {
    c = substr(header, i, 1)
    if (c == "<") open++
    else if (c == ">" && --open == 0) break
  }
  return substr(header, at, i - at)
}
function count_char(s, ch,   n, i) {
  n = 0
  for (i = 1; i <= length(s); i++) if (substr(s, i, 1) == ch) n++
  return n
}

# A signature is fed one code line at a time from its `fn` keyword: the
# generics and parameters go to sig_in, everything after the closing paren to
# sig_tail, and it ends at a `{` or `;` outside brackets (`[u8; 4]` is a type).
function sig_feed(s,   i, n, c) {
  n = length(s)
  for (i = 1; i <= n; i++) {
    c = substr(s, i, 1)
    if (sig_phase == 0) {
      if (c == "(") { sig_phase = 1; sig_depth = 1 }
      if (c == ";" || c == "{") { sig_done = 1; return }
      sig_in = sig_in c; continue
    }
    if (sig_phase == 1) {
      if (c == "(") sig_depth++
      else if (c == ")") {
        sig_depth--
        if (sig_depth == 0) { sig_phase = 2; continue }
      }
      sig_in = sig_in c; continue
    }
    if (c == "[") sig_bdepth++
    else if (c == "]") sig_bdepth--
    if ((c == "{" || c == ";") && sig_bdepth <= 0) { sig_done = 1; return }
    sig_tail = sig_tail c
  }
  sig_in = sig_in " "
  sig_tail = sig_tail " "
}
function sig_start(code, at, name, marked) {
  sig_open = 1; sig_done = 0; sig_phase = 0; sig_depth = 0; sig_bdepth = 0
  sig_in = ""; sig_tail = ""
  sig_line = FNR; sig_name = name; sig_marked = marked
  sig_feed(substr(code, at))
}
function sig_finish(   where_at, text) {
  sig_open = 0
  text = sig_in
  where_at = match(sig_tail, /(^|[^A-Za-z0-9_])where([^A-Za-z0-9_]|$)/)
  if (where_at > 0) text = text " " substr(sig_tail, where_at)
  if (FILENAME == ANCHOR_PARSE && sig_name == "parse" && !sig_in_trait) {
    parse_defs++
    if (takes_text(text)) parse_reads++
    return
  }
  if (FILENAME == ANCHOR_LEX && sig_name == "lex" && !sig_in_trait) {
    lex_defs++
    if (takes_text(text)) lex_reads++
    return
  }
  if (takes_input(text) && !sig_marked) {
    printf "%s:%d: source-entry: public `%s` takes %s; only" \
           " ravel_esql::parse and ravel_esql::lex may, because they apply" \
           " the limits (issue #2892). Make it pub(crate), route it through" \
           " parse or lex, or mark the line with `// %s <reason>`\n", \
           FILENAME, sig_line, sig_name, \
           (takes_text(text) ? "source text" : "lexed tokens"), MARKER
    findings++
  }
}

# What a completed `use` brings into scope. In the crate: aliases of the
# entries. In a caller: aliases of the crate, and a glob import of it.
function record_use(buf,   rest, m, parts, n) {
  if (in_crate) {
    if (buf !~ /(^|[^A-Za-z0-9_])pub[ \t]+use([^A-Za-z0-9_]|$)/) return
    rest = buf
    while (match(rest, /(^|[^A-Za-z0-9_])(lex|parse)[ \t]+as[ \t]+[A-Za-z_][A-Za-z0-9_]*/)) {
      m = substr(rest, RSTART, RLENGTH)
      n = split(m, parts, /[ \t]+/)
      allowed[parts[n]] = 1
      rest = substr(rest, RSTART + RLENGTH)
    }
    return
  }
  if (buf !~ crate_re) return
  if (match(buf, /ravel_esql[ \t]+as[ \t]+[A-Za-z_][A-Za-z0-9_]*/)) {
    m = substr(buf, RSTART, RLENGTH)
    n = split(m, parts, /[ \t]+/)
    crate_names[parts[n]] = 1
    rebuild_crate_re()
  }
  # `ravel_esql::*` or a `*` directly in its top-level braces; `ast::*`
  # brings in no module of the crate.
  if (buf ~ ("(" crate_alt() ")[ \t]*::[ \t]*(\\*|\\{[ \t]*\\*)") ||
      (buf ~ ("(" crate_alt() ")[ \t]*::[ \t]*\\{") && buf ~ /,[ \t]*\*/)) glob_import = 1
}
function rebuild_crate_re() {
  crate_re = "(^|[^A-Za-z0-9_])(" crate_alt() ")([^A-Za-z0-9_]|$)"
}

# Every `lexer`/`parser` segment in s that `prefix_re` admits, checked for
# what follows it. Returns 1 when one does not end at an allowed entry.
function bad_module_path(s, prefix_re,   rest, after, name) {
  rest = s
  while (match(rest, prefix_re "(lexer|parser)")) {
    after = substr(rest, RSTART + RLENGTH)
    rest = after
    if (after ~ /^[A-Za-z0-9_]/) continue
    if (!match(after, /^[ \t]*::[ \t]*[A-Za-z_][A-Za-z0-9_]*/)) return 1
    name = substr(after, RSTART, RLENGTH)
    sub(/^[ \t]*::[ \t]*/, "", name)
    if (!(name in allowed)) return 1
    if (substr(after, RLENGTH + 1) ~ /^[ \t]*::/) return 1
  }
  return 0
}
function names_entry(s,   prefix) {
  prefix = "(" crate_alt() ")[ \t]*::[ \t]*((lexer|parser)[ \t]*::[ \t]*)?"
  return s ~ (prefix "(" allowed_alt() ")([^A-Za-z0-9_]|$)")
}
function crate_alt(   name, alt) {
  alt = ""
  for (name in crate_names) alt = alt (alt == "" ? "" : "|") name
  return alt
}
function allowed_alt(   name, alt) {
  alt = ""
  for (name in allowed) alt = alt (alt == "" ? "" : "|") name
  return alt
}

BEGIN {
  SQ = sprintf("%c", 39)
  MARKER = "guarded-esql-parse-allow:"
  findings = 0
  parse_defs = 0; parse_reads = 0; lex_defs = 0; lex_reads = 0
  entry_refs = 0
  allowed["parse"] = 1
  allowed["lex"] = 1
}
FNR == 1 {
  if (sig_open) sig_finish()
  sstate = 0; sesc = 0; shashes = 0; bdepth = 0; block_marker = 0
  use_open = 0; use_buf = ""
  depth = 0; trait_pending = 0; trait_open = 0; trait_base = 0
  impl_open = 0; impl_buf = ""
  in_crate = (index(FILENAME, CRATE_ROOT) == 1)
  split("", crate_names)
  crate_names["ravel_esql"] = 1
  rebuild_crate_re()
  glob_import = 0
}
{
  code = strip_line($0)
  marked = has_marker(cmt) || block_marker

  if (!use_open && code ~ /(^|[^A-Za-z0-9_])use[ \t]/) { use_open = 1; use_buf = "" }
  if (!use_open && code ~ /(^|[^A-Za-z0-9_])extern[ \t]+crate[ \t]/) { use_open = 1; use_buf = "" }
  if (use_open) use_buf = use_buf " " code

  if (in_crate) {
    if (sig_open) {
      sig_feed(code)
      if (sig_done) sig_finish()
    } else if (match(code, /(^|[^A-Za-z0-9_])fn[ \t]+[A-Za-z_][A-Za-z0-9_]*/)) {
      fn_at = RSTART
      fn_text = substr(code, RSTART, RLENGTH)
      sub(/^[^A-Za-z_]*fn[ \t]+/, "", fn_text)
      head = substr(code, 1, fn_at)
      is_pub = (head ~ /(^|[^A-Za-z0-9_])pub[ \t]+([A-Za-z]+[ \t]+)*$/)
      sig_in_trait = (trait_open && depth == trait_base + 1)
      if (is_pub || sig_in_trait) {
        sig_start(code, fn_at, fn_text, marked)
        if (sig_done) sig_finish()
      }
    }

    if (!impl_open && code ~ /(^|[^A-Za-z0-9_])impl([ \t<]|$)/) {
      impl_open = 1; impl_buf = ""; impl_line = FNR; impl_marked = marked
    }
    if (impl_open) {
      impl_buf = impl_buf " " code
      if (impl_buf ~ /[{;]/) {
        impl_open = 0
        header = impl_buf
        sub(/[{;].*/, "", header)
        if ((header ~ /(^|[^A-Za-z0-9_])FromStr([^A-Za-z0-9_]|$)/ ||
             takes_input(from_arg(header))) &&
            header ~ /(^|[^A-Za-z0-9_])for([^A-Za-z0-9_]|$)/ && !impl_marked) {
          printf "%s:%d: source-entry: a conversion from source text is a" \
                 " public entry that skips the limits; only ravel_esql::parse" \
                 " and ravel_esql::lex may take it (issue #2892), or mark the" \
                 " line with `// %s <reason>`\n", FILENAME, impl_line, MARKER
          findings++
        }
      }
    }

    if (code ~ /(^|[^A-Za-z0-9_])pub[ \t]+([A-Za-z]+[ \t]+)*trait[ \t]/) {
      trait_pending = 1; trait_base = depth
    }
    depth += count_char(code, "{") - count_char(code, "}")
    if (trait_pending && depth > trait_base) { trait_pending = 0; trait_open = 1 }
    if (trait_open && depth <= trait_base) trait_open = 0
  } else {
    found = 0
    in_use = use_open && use_buf ~ crate_re
    if (bad_module_path(code, "(" crate_alt() ")[ \t]*::[ \t]*")) found = 1
    if (!found && in_use && bad_module_path(code, "(^|[^A-Za-z0-9_])")) found = 1
    if (!found && glob_import && bad_module_path(code, "(^|[^A-Za-z0-9_:])")) found = 1
    if (names_entry(code)) entry_refs++
    else if (in_use && code ~ ("(^|[^A-Za-z0-9_])(" allowed_alt() ")([^A-Za-z0-9_]|$)")) entry_refs++
    if (found && !marked) {
      printf "%s:%d: unguarded-reference: reaches a ravel_esql lexer or" \
             " parser item other than ravel_esql::parse and ravel_esql::lex," \
             " which apply the limits (issue #2892); use one of them, or mark" \
             " the line with `// %s <reason>`\n", FILENAME, FNR, MARKER
      findings++
    }
  }

  if (use_open && code ~ /;/) { record_use(use_buf); use_open = 0 }

  if ($0 ~ /^[ \t]*\/\//) {
    if (has_marker(cmt)) block_marker = 1
  } else {
    block_marker = 0
  }
}
END {
  if (sig_open) sig_finish()
  if (parse_defs != 1 || lex_defs != 1) {
    printf "anchor: expected exactly one `pub fn parse` in %s and one" \
           " `pub fn lex` in %s, found %d and %d. Without them this guard" \
           " passes everything: point it at the guarded entries again" \
           " (issue #2892)\n", ANCHOR_PARSE, ANCHOR_LEX, parse_defs, lex_defs > "/dev/stderr"
    exit 2
  }
  if (parse_reads != 1 || lex_reads != 1) {
    printf "anchor: `parse` or `lex` no longer reads as taking source text," \
           " so the rule that finds other entries is looking for the wrong" \
           " types (issue #2892)\n" > "/dev/stderr"
    exit 2
  }
  if (entry_refs == 0) {
    printf "anchor: the caller roots name neither ravel_esql::parse nor" \
           " ravel_esql::lex. Either the callers reach the crate through" \
           " something this guard does not read (re-point it) or they no" \
           " longer use it (drop the roots); a root scanned for a rule it" \
           " takes no part in reports clean whatever it holds (issue #2892)\n" \
           > "/dev/stderr"
    exit 2
  }
  if (findings > 0) exit 1
  exit 0
}
'

awk -v ANCHOR_PARSE="${anchor_parse_file}" -v ANCHOR_LEX="${anchor_lex_file}" \
  -v CRATE_ROOT="${crate_root}" "${awk_prog}" "${crate_sources[@]}" "${caller_sources[@]}"
rc=$?
if [[ ${rc} -eq 0 ]]; then
  echo "check-guarded-esql-parse.sh: only parse and lex take ES|QL source text"
fi
exit ${rc}
