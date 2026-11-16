#!/usr/bin/env bash
# The lane-area map behind ci.yml's `changes` job (ADR-4034 D1, issue #4050).
#
# The `changes` job decides which lane jobs run from the edited paths alone:
# it has no Rust toolchain, so it cannot run `affected-tests.sh -n`. An edit
# under crates/ravel-ingest/ reaches ravel-server's lanes through the
# reverse-dependency closure, and a path rule does not see that. So
# scripts/ci-lane-areas.tsv maps each workspace crate's directory to the lane
# areas its closure reaches, and the `changes` job sets a lane's area when an
# edited path is under a row naming that lane. This script keeps the map
# honest against `affected-tests.sh -n`, which stays the one definition of
# the closure.
#
# A lane is reached when its closure holds one of the crates the lane tests,
# the same selection scripts/gates.sh makes for its scoped lanes:
#
#   sql         ravel-server, ravel-sql, ravel-esql  (CI's `sql`: the server's
#               esql suite and ravel-sql's differential gate)
#   flight-sql  ravel-server, ravel-sql
#   agents      ravel-server, ravel-agents-runtime, ravel-agents-sim
#   mcp         ravel-server
#   promql      ravel-promql-difftest  (CI's `promql-difftest`, a required
#               check gated on paths like the four above)
#
# Modes:
#   check-ci-lane-areas.sh [--map FILE]
#       Recompute every crate's row from `affected-tests.sh -n` and compare
#       it with the map. Each difference is a finding naming the crate and
#       the area: a workspace crate with no row, a row whose directory is no
#       workspace crate, a lane the row names that the closure does not
#       reach, and a lane the closure reaches that the row omits. Needs
#       `cargo metadata` and jq; builds nothing.
#   check-ci-lane-areas.sh --generate
#       Print the map as the check computes it.
#   check-ci-lane-areas.sh --lanes-for-paths [--map FILE]
#       Read changed paths on stdin, one per line, and print each lane (in
#       the order above, once each) named by a row whose directory holds one
#       of them. Plain awk; this is what the `changes` job runs.
#
# Map format: `#` comment lines, then one row per workspace crate, sorted by
# directory: `<crate directory relative to the workspace root><TAB><lanes>`,
# lanes comma-separated in the order above, or `-` for none.
#
# RAVEL_AFFECTED_TESTS overrides the closure script (default:
# scripts/affected-tests.sh beside this script's directory).
#
# Exit codes: 0 clean (check) or done (other modes); 1 a finding; 2 could
# not answer (the map is missing or empty, cargo metadata or the closure
# script failed or printed no closure, jq is missing, a map line does not
# parse in --lanes-for-paths); 64 usage.
set -uo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SELF_DIR}/../.." && pwd)"
AFFECTED_TESTS="${RAVEL_AFFECTED_TESTS:-${REPO_ROOT}/scripts/affected-tests.sh}"
LANES="sql flight-sql agents mcp promql"

usage() {
  echo "usage: $0 [--map FILE] | --generate | --lanes-for-paths [--map FILE]" >&2
  exit 64
}

mode="check"
map="${REPO_ROOT}/scripts/ci-lane-areas.tsv"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --map)
      [[ $# -ge 2 ]] || usage
      map="$2"
      shift 2
      ;;
    --generate)
      mode="generate"
      shift
      ;;
    --lanes-for-paths)
      mode="lookup"
      shift
      ;;
    *) usage ;;
  esac
done

# lanes_for_closure <closure>: the comma-separated lanes a closure (a list of
# crate names, space-separated) reaches, or `-`.
lanes_for_closure() {
  local closure=" $1 " out=""
  case "${closure}" in
    *" ravel-server "* | *" ravel-sql "* | *" ravel-esql "*) out="${out},sql" ;;
  esac
  case "${closure}" in
    *" ravel-server "* | *" ravel-sql "*) out="${out},flight-sql" ;;
  esac
  case "${closure}" in
    *" ravel-server "* | *" ravel-agents-runtime "* | *" ravel-agents-sim "*) out="${out},agents" ;;
  esac
  case "${closure}" in
    *" ravel-server "*) out="${out},mcp" ;;
  esac
  case "${closure}" in
    *" ravel-promql-difftest "*) out="${out},promql" ;;
  esac
  if [[ -z "${out}" ]]; then
    printf '%s' "-"
  else
    printf '%s' "${out#,}"
  fi
}

if [[ "${mode}" == "lookup" ]]; then
  if [[ ! -s "${map}" ]] || ! grep -qv '^#' "${map}"; then
    echo "check-ci-lane-areas.sh: map ${map} is missing or has no rows" >&2
    exit 2
  fi
  awk -F'\t' -v lanes="${LANES}" '
    NR == FNR {
      if ($0 ~ /^#/ || $0 == "") next
      if (NF != 2 || $1 == "" || $2 == "") {
        printf "check-ci-lane-areas.sh: map line %d does not parse: %s\n", FNR, $0 > "/dev/stderr"
        bad = 1
        exit
      }
      n++
      dir[n] = $1 "/"
      row[n] = $2
      next
    }
    {
      for (i = 1; i <= n; i++) {
        if (index($0, dir[i]) == 1) {
          k = split(row[i], l, ",")
          for (j = 1; j <= k; j++) if (l[j] != "-") hit[l[j]] = 1
        }
      }
    }
    END {
      if (bad) exit 2
      m = split(lanes, order, " ")
      for (i = 1; i <= m; i++) if (order[i] in hit) print order[i]
    }
  ' "${map}" -
  exit $?
fi

command -v jq >/dev/null 2>&1 || {
  echo "check-ci-lane-areas.sh: jq is required" >&2
  exit 2
}
if ! meta="$(cd "${REPO_ROOT}" && cargo metadata --format-version 1 --no-deps)"; then
  echo "check-ci-lane-areas.sh: cargo metadata failed" >&2
  exit 2
fi
# "<directory relative to the workspace root><TAB><crate name>", by directory.
crates="$(printf '%s' "${meta}" | jq -r '
  .workspace_root as $r
  | .packages[]
  | "\(.manifest_path | sub("/Cargo\\.toml$"; "") | ltrimstr($r + "/"))\t\(.name)"' | LC_ALL=C sort)"
if [[ -z "${crates}" ]]; then
  echo "check-ci-lane-areas.sh: cargo metadata listed no workspace crate" >&2
  exit 2
fi

generated=""
while IFS=$'\t' read -r dir name; do
  [[ -z "${dir}" ]] && continue
  if ! out="$(cd "${REPO_ROOT}" && bash "${AFFECTED_TESTS}" -n -p "${name}")"; then
    echo "check-ci-lane-areas.sh: ${AFFECTED_TESTS} -n -p ${name} failed" >&2
    exit 2
  fi
  closure="$(printf '%s\n' "${out}" | sed -n 's/^affected crates://p')"
  if [[ -z "${closure// /}" ]]; then
    echo "check-ci-lane-areas.sh: ${AFFECTED_TESTS} -n -p ${name} printed no closure" >&2
    exit 2
  fi
  generated="${generated}${dir}"$'\t'"$(lanes_for_closure "${closure}")"$'\n'
done <<<"${crates}"

if [[ "${mode}" == "generate" ]]; then
  echo "# Lane areas each workspace crate's reverse-dependency closure reaches."
  echo "# Generated by scripts/guards/check-ci-lane-areas.sh --generate from"
  echo "# scripts/affected-tests.sh -n; that script's check mode fails on any"
  echo "# difference. Read by the changes job in .github/workflows/ci.yml."
  printf '%s' "${generated}"
  exit 0
fi

if [[ ! -s "${map}" ]] || ! grep -qv '^#' "${map}"; then
  echo "check-ci-lane-areas.sh: map ${map} is missing or has no rows" >&2
  exit 2
fi

findings=0
finding() {
  echo "check-ci-lane-areas.sh: FINDING $*"
  findings=$((findings + 1))
}

# map_row <dir>: the lanes field of the map's row for <dir>, empty when none.
map_row() {
  awk -F'\t' -v d="$1" '!/^#/ && $1 == d { print $2; exit }' "${map}"
}

while IFS=$'\t' read -r dir want; do
  [[ -z "${dir}" ]] && continue
  name="$(awk -F'\t' -v d="${dir}" '$1 == d { print $2; exit }' <<<"${crates}")"
  have="$(map_row "${dir}")"
  if [[ -z "${have}" ]]; then
    finding "crate ${name} (${dir}) has no row in ${map}; its closure reaches: ${want}"
    continue
  fi
  for lane in ${LANES}; do
    case ",${want}," in *",${lane},"*) reach=1 ;; *) reach=0 ;; esac
    case ",${have}," in *",${lane},"*) named=1 ;; *) named=0 ;; esac
    if ((named == 1 && reach == 0)); then
      finding "crate ${name} (${dir}): the map names area ${lane}, which its closure does not reach"
    elif ((named == 0 && reach == 1)); then
      finding "crate ${name} (${dir}): its closure reaches area ${lane}, which the map row omits"
    fi
  done
  for lane in ${have//,/ }; do
    case " ${LANES} - " in
      *" ${lane} "*) ;;
      *) finding "crate ${name} (${dir}): the map names unknown area ${lane}" ;;
    esac
  done
done <<<"${generated}"

while IFS=$'\t' read -r dir _; do
  [[ -z "${dir}" || "${dir}" == \#* ]] && continue
  if ! awk -F'\t' -v d="${dir}" '$1 == d { found = 1 } END { exit !found }' <<<"${generated}"; then
    finding "map row ${dir} is not a workspace crate directory"
  fi
done <"${map}"

if ((findings > 0)); then
  echo "check-ci-lane-areas.sh: ${findings} finding(s); regenerate with scripts/guards/check-ci-lane-areas.sh --generate > scripts/ci-lane-areas.tsv"
  exit 1
fi
echo "check-ci-lane-areas.sh: clean ($(grep -c . <<<"${generated}") crates match ${map})"
exit 0
