#!/usr/bin/env bash
# Workspace version-drift guard (ADR-0086 decision 8).
#
# Failure class this prevents: a version bump that updates
# `[workspace.package] version` in the root Cargo.toml but leaves one or more
# path dependencies pinned to the old version. Epic #218 shipped a 0.9.3 bump
# that left seven per-crate path dependencies requiring 0.9.2. That compiles,
# because a caret requirement on 0.9.2 is satisfied by a 0.9.3 path crate, so
# the drift stays invisible until a MINOR bump makes those requirements fail to
# resolve, landing the failure on whoever cuts 0.10.0, far from the change that
# caused it.
#
# This asserts the invariant directly: every `version = "..."` attached to a
# path dependency, in every workspace manifest, must equal the root
# `[workspace.package] version`. It reads Cargo.toml text only; no cargo build,
# no toolchain.
#
# Exit 0 when every path-dependency version matches, 1 on drift (each mismatch
# named as file:line), 2 when the workspace version cannot be read at all.
#
# Portability: must run on bash 3.2 (macOS system bash) as well as bash 5, so
# no `mapfile` and no associative arrays (issue #193). The line-by-line parsing
# is done in awk for the same reason.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
root_manifest=$repo_root/Cargo.toml

fail_setup() {
  echo "check-workspace-versions: $1" >&2
  exit 2
}

[[ -r $root_manifest ]] || fail_setup "cannot read $root_manifest"

# The authoritative version: the `version` key inside the [workspace.package]
# table, not [workspace.dependencies] or any other section. awk tracks the
# current table header so a `version` line elsewhere can never be mistaken for
# it.
ws_version=$(awk '
  /^\[/ { in_pkg = ($0 == "[workspace.package]"); next }
  in_pkg && match($0, /^[[:space:]]*version[[:space:]]*=[[:space:]]*"[^"]*"/) {
    v = substr($0, RSTART, RLENGTH)
    match(v, /"[^"]*"/)
    print substr(v, RSTART + 1, RLENGTH - 2)
    exit
  }
' "$root_manifest")
[[ -n $ws_version ]] ||
  fail_setup "no [workspace.package] version in $root_manifest"

# Every tracked Cargo.toml in the workspace. `git ls-files` keeps vendored,
# target/, and node_modules manifests out of scope without a prune list.
manifests=$(git -C "$repo_root" ls-files '*Cargo.toml' 'Cargo.toml')
[[ -n $manifests ]] || fail_setup "no Cargo.toml files tracked in $repo_root"

# Scan one manifest, printing "<file>:<line>: ..." for each path dependency
# whose version differs from the workspace version. Handles both dependency
# spellings this repo could use:
#   - inline table:  foo = { path = "../foo", version = "0.9.3" }
#   - table header:  [dependencies.foo] / [dev-dependencies.foo] with `path`
#     and `version` on separate lines.
# A path KEY is matched only when preceded by `{`, `,`, or line start, so a
# crate whose NAME ends in "path" (fastpath = { version = "1" }) is not a false
# positive, and a `[[bin]]` `path = "src/..."` target carries no version so it
# is never checked. Exit status: 1 if any drift was printed, else 0.
scan_manifest() {
  awk -v ws="$ws_version" -v file="$1" '
    function verval(s,   v) {
      # Return the quoted value of the first `version = "..."` on the line.
      if (match(s, /version[[:space:]]*=[[:space:]]*"[^"]*"/)) {
        v = substr(s, RSTART, RLENGTH)
        match(v, /"[^"]*"/)
        return substr(v, RSTART + 1, RLENGTH - 2)
      }
      return ""
    }
    function report(ver) {
      printf "%s:%d: path dependency version \"%s\" != workspace version \"%s\"\n", \
        file, FNR, ver, ws
      bad = 1
    }
    # Flush a pending multiline dependency table before moving on.
    function flush(   v) {
      if (tbl_is_dep && tbl_path && tbl_ver != "") {
        if (tbl_ver != ws) {
          printf "%s:%d: path dependency version \"%s\" != workspace version \"%s\"\n", \
            file, tbl_ver_line, tbl_ver, ws
          bad = 1
        }
      }
      tbl_is_dep = 0; tbl_path = 0; tbl_ver = ""; tbl_ver_line = 0
    }
    /^\[/ {
      flush()
      # A single-dependency table: [dependencies.NAME], [dev-dependencies.NAME],
      # [build-dependencies.NAME], or a target-scoped variant of any of them.
      tbl_is_dep = ($0 ~ /^\[([^]]*\.)?(dependencies|dev-dependencies|build-dependencies)\.[^]]+\][[:space:]]*$/)
      next
    }
    # Inline-table path dependency: a `path` key inside `{ ... }` on this line,
    # plus a `version` key on the same line.
    /[{,[:space:]]path[[:space:]]*=/ && /version[[:space:]]*=/ {
      ver = verval($0)
      if (ver != "" && ver != ws) report(ver)
      next
    }
    # Multiline dependency-table body: accumulate path/version until the table
    # ends (next header or EOF), then compare in flush().
    tbl_is_dep && /^[[:space:]]*path[[:space:]]*=/ { tbl_path = 1; next }
    tbl_is_dep && /^[[:space:]]*version[[:space:]]*=/ {
      tbl_ver = verval($0); tbl_ver_line = FNR; next
    }
    END { flush(); exit bad }
  ' "$1"
}

drift=0
while IFS= read -r manifest; do
  [[ -n $manifest ]] || continue
  scan_manifest "$repo_root/$manifest" || drift=1
done <<EOF
$manifests
EOF

if ((drift)); then
  echo "check-workspace-versions: path dependency versions drifted from [workspace.package] version \"$ws_version\"; update the versions listed above (ADR-0086 decision 8)" >&2
  exit 1
fi

echo "check-workspace-versions: all path dependency versions match [workspace.package] version \"$ws_version\""
