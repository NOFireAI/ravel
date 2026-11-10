#!/usr/bin/env bash
# Cases for check-ci-lane-areas.sh (ADR-4034 D1, issue #4050). Add a case here
# before changing a rule.
#
# The lookup cases read the checked-in scripts/ci-lane-areas.tsv, the map the
# `changes` job reads, so they pin the areas a real edit sets. The check cases
# need no cargo: a stub `cargo` on PATH prints a fixture workspace's
# `cargo metadata`, and the real scripts/affected-tests.sh computes the
# closure from it, so the comparison runs against a known graph.
#
# CI_LANE_AREAS_GUARD and CI_LANE_AREAS_MAP point the suite at another guard
# or map; that is how each FLIP below was shown failing.
#
# Run: bash scripts/guards/check-ci-lane-areas.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
GUARD="${CI_LANE_AREAS_GUARD:-${HERE}/check-ci-lane-areas.sh}"
MAP="${CI_LANE_AREAS_MAP:-${REPO}/scripts/ci-lane-areas.tsv}"
export RAVEL_AFFECTED_TESTS="${REPO}/scripts/affected-tests.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-ci-lane-areas-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

ok() {
  printf 'ok    %s\n' "$1"
  passes=$((passes + 1))
}

not_ok() {
  printf 'FAIL  %s: %s\n' "$1" "$2"
  fails=$((fails + 1))
}

# lanes_for <path...>: the lanes the map gives the paths, space-separated.
lanes_for() {
  local out rc=0
  out="$(printf '%s\n' "$@" | bash "${GUARD}" --lanes-for-paths --map "${MAP}")" || rc=$?
  if [[ "${rc}" -ne 0 ]]; then
    printf 'exit=%s' "${rc}"
    return
  fi
  printf '%s' "${out}" | tr '\n' ' ' | sed 's/ $//'
}

# --- lookup against the checked-in map ---------------------------------------

# FLIP: give crates/ravel-ingest's row `-` in a copy of the map (what today's
# path rules say for it, object_store_area only) and this prints nothing.
got="$(lanes_for crates/ravel-ingest/src/lib.rs)"
if [[ "${got}" == "sql flight-sql agents mcp promql" ]]; then
  ok "edit under crates/ravel-ingest/ sets sql, flight-sql, agents, mcp and promql"
else
  not_ok "edit under crates/ravel-ingest/ sets sql, flight-sql, agents, mcp and promql" "got '${got}'"
fi

# FLIP: make the lookup fall back to every lane when no row holds a path (a
# "conservative" default for unmapped paths) and this prints all four.
got="$(lanes_for docs/ingest.md docs/adrs/4034-dispatch-to-merge-latency.md)"
if [[ -z "${got}" ]]; then
  ok "edit under docs/ sets none of the lanes"
else
  not_ok "edit under docs/ sets none of the lanes" "got '${got}'"
fi

# FLIP: drop mcp from services/ravel-server's row in a copy of the map and
# this prints sql flight-sql agents.
got="$(lanes_for services/ravel-server/src/main.rs)"
if [[ "${got}" == "sql flight-sql agents mcp" ]]; then
  ok "edit under services/ravel-server/ sets all four lanes"
else
  not_ok "edit under services/ravel-server/ sets all four lanes" "got '${got}'"
fi

# FLIP: match a row's directory without its trailing slash and the
# crates/ravel-sql row also claims crates/ravel-sqlx/, setting all four.
got="$(lanes_for crates/ravel-sqlx/src/lib.rs)"
if [[ -z "${got}" ]]; then
  ok "a path matches a row only under the row's directory"
else
  not_ok "a path matches a row only under the row's directory" "got '${got}'"
fi

# --- the changes job's own step, run on a given list of edited paths ---------

# The `changes` job's `check` step from ci.yml, with its GitHub expressions
# replaced so it reads a pull request whose diff is $CHANGED. Runs from the
# repository root, where the step finds the guard and the map.
STEP="${TMP}/changes-step.sh"
# shellcheck disable=SC2016 # the sed scripts write shell text, unexpanded
sed -n '/^      - id: check$/,/^$/p' "${REPO}/.github/workflows/ci.yml" \
  | sed '1,2d; s/^          //' \
  | sed 's/\${{ github\.event_name }}/pull_request/; s/\${{ github\.event\.pull_request\.base\.sha }}/HEAD/; s/\${{ github\.event\.before }}/HEAD/; s/\${{ github\.sha }}/HEAD/g' \
  | sed 's/^changed=\$(git diff .*/changed="${CHANGED}"/' >"${STEP}"

# areas_for <path...>: the step's four lane outputs, as name=value words.
areas_for() {
  local out="${TMP}/github-output" rc=0
  : >"${out}"
  (cd "${REPO}" && CHANGED="$(printf '%s\n' "$@")" GITHUB_OUTPUT="${out}" bash -e "${STEP}" >/dev/null 2>&1) || rc=$?
  if [[ "${rc}" -ne 0 ]]; then
    printf 'exit=%s' "${rc}"
    return
  fi
  grep -E '^(sql_area|flight_sql_area|agents_area|mcp_area)=' "${out}" | tr '\n' ' ' | sed 's/ $//'
}

ALL="sql_area=true agents_area=true flight_sql_area=true mcp_area=true"
NONE="sql_area=false agents_area=false flight_sql_area=false mcp_area=false"

# FLIP: turn the step's `mcp) mcp_area=true ;;` arm into `mcp) ;;` and
# mcp_area stays false.
got="$(areas_for crates/ravel-ingest/src/lib.rs)"
if [[ "${got}" == "${ALL}" ]]; then
  ok "changes step: edit under crates/ravel-ingest/ sets all four areas"
else
  not_ok "changes step: edit under crates/ravel-ingest/ sets all four areas" "got '${got}'"
fi

# FLIP: drop the step's `promql) promql_area=true ;;` arm and the step exits
# 1 on the map's promql lane as an unknown lane; drop promql from the map's
# ingest row and promql_area reads false, since no path rule sets it for an
# edit confined to crates/ravel-ingest/ though ravel-promql-difftest depends
# on ravel-ingest. The guard's own promql arm is pinned by the check cases
# below, whose stubbed workspace holds ravel-promql-difftest.
promql_for() {
  local out="${TMP}/github-output-promql" rc=0
  : >"${out}"
  (cd "${REPO}" && CHANGED="$(printf '%s\n' "$@")" GITHUB_OUTPUT="${out}" bash -e "${STEP}" >/dev/null 2>&1) || rc=$?
  if [[ "${rc}" -ne 0 ]]; then
    printf 'exit=%s' "${rc}"
    return
  fi
  grep -E '^promql_area=' "${out}"
}
got="$(promql_for crates/ravel-ingest/src/lib.rs)"
if [[ "${got}" == "promql_area=true" ]]; then
  ok "changes step: edit under crates/ravel-ingest/ sets promql_area"
else
  not_ok "changes step: edit under crates/ravel-ingest/ sets promql_area" "got '${got}'"
fi
got="$(promql_for docs/ingest.md)"
if [[ "${got}" == "promql_area=false" ]]; then
  ok "changes step: edit under docs/ leaves promql_area false"
else
  not_ok "changes step: edit under docs/ leaves promql_area false" "got '${got}'"
fi

# FLIP: emit every area true on the docs/formal-only branch and this reads
# all four true.
got="$(areas_for docs/ingest.md)"
if [[ "${got}" == "${NONE}" ]]; then
  ok "changes step: edit under docs/ sets none of the four areas"
else
  not_ok "changes step: edit under docs/ sets none of the four areas" "got '${got}'"
fi

# FLIP: delete services/ravel-server's row from the map and agents_area and
# mcp_area read false: no path rule sets either for src/main.rs.
got="$(areas_for services/ravel-server/src/main.rs)"
if [[ "${got}" == "${ALL}" ]]; then
  ok "changes step: edit under services/ravel-server/ sets all four areas"
else
  not_ok "changes step: edit under services/ravel-server/ sets all four areas" "got '${got}'"
fi

# FLIP: set all four areas whenever the map names any lane for a path and
# this reads all four true: ravel-agents-sim's closure reaches agents alone.
got="$(areas_for crates/ravel-agents-sim/src/lib.rs)"
if [[ "${got}" == "sql_area=false agents_area=true flight_sql_area=false mcp_area=false" ]]; then
  ok "changes step: edit under crates/ravel-agents-sim/ sets agents alone"
else
  not_ok "changes step: edit under crates/ravel-agents-sim/ sets agents alone" "got '${got}'"
fi

# --- the check, against a stubbed workspace ----------------------------------

WS="${TMP}/ws"
mkdir -p "${TMP}/bin" "${WS}"

# pkg <name> <dir> <dep...>: one package object of `cargo metadata --no-deps`.
pkg() {
  local name="$1" dir="$2" deps="" d
  shift 2
  for d in "$@"; do
    deps="${deps}${deps:+,}{\"name\":\"${d}\"}"
  done
  printf '{"name":"%s","manifest_path":"%s/%s/Cargo.toml","dependencies":[%s]}' \
    "${name}" "${WS}" "${dir}" "${deps}"
}

{
  printf '{"workspace_root":"%s","packages":[' "${WS}"
  pkg lone crates/lone
  printf ','
  pkg ravel-types crates/ravel-types
  printf ','
  pkg ravel-ingest crates/ravel-ingest ravel-types
  printf ','
  pkg ravel-sql crates/ravel-sql ravel-ingest
  printf ','
  pkg ravel-agents-sim crates/ravel-agents-sim
  printf ','
  pkg ravel-promql-difftest crates/ravel-promql-difftest ravel-ingest
  printf ','
  pkg ravel-server services/ravel-server ravel-sql ravel-ingest
  printf ']}\n'
} >"${TMP}/metadata.json"

cat >"${TMP}/bin/cargo" <<EOF
#!/usr/bin/env bash
if [[ "\$1" == "metadata" ]]; then
  cat "${TMP}/metadata.json"
  exit 0
fi
echo "stub cargo: unexpected \$*" >&2
exit 99
EOF
chmod +x "${TMP}/bin/cargo"

GOOD="$(printf '%s\t%s\n' \
  crates/lone - \
  crates/ravel-agents-sim agents \
  crates/ravel-ingest sql,flight-sql,agents,mcp,promql \
  crates/ravel-promql-difftest promql \
  crates/ravel-sql sql,flight-sql,agents,mcp \
  crates/ravel-types sql,flight-sql,agents,mcp,promql \
  services/ravel-server sql,flight-sql,agents,mcp)"

# check_map <name> <map-text> <want-exit> <want-substring...>
check_map() {
  local name="$1" text="$2" want_rc="$3" out rc=0 sub
  shift 3
  printf '%s\n' "${text}" >"${TMP}/map.tsv"
  out="$(PATH="${TMP}/bin:${PATH}" bash "${GUARD}" --map "${TMP}/map.tsv" 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    not_ok "${name}" "exit ${rc}, wanted ${want_rc}; output: ${out}"
    return
  fi
  for sub in "$@"; do
    if [[ "${out}" != *"${sub}"* ]]; then
      not_ok "${name}" "output missing '${sub}': ${out}"
      return
    fi
  done
  ok "${name}"
}

# FLIP: compute lanes from direct dependants only (not the fixed-point
# closure) and ravel-types, two hops from ravel-server, reaches nothing:
# exit 1 with five findings on the ravel-types row.
check_map "a map matching the closure passes" "${GOOD}" 0 "clean (7 crates"

# FLIP: walk the map's rows instead of the workspace's crates and a crate with
# no row is never asked about: exit 0.
check_map "a crate missing from the map fails" \
  "$(grep -v '^crates/ravel-ingest	' <<<"${GOOD}")" 1 \
  "crate ravel-ingest (crates/ravel-ingest) has no row" "reaches: sql,flight-sql,agents,mcp"

# FLIP: check only that the closure's lanes are in the row (reached implies
# named) and a row naming more than the closure reaches passes: exit 0.
check_map "a row naming a lane the closure does not reach fails" \
  "$(sed 's/^crates\/ravel-agents-sim	agents$/crates\/ravel-agents-sim	sql,agents/' <<<"${GOOD}")" 1 \
  "crate ravel-agents-sim (crates/ravel-agents-sim): the map names area sql, which its closure does not reach"

# FLIP: check only that the row's lanes are reached (named implies reached)
# and a row omitting a reached lane passes: exit 0.
check_map "a row omitting a lane the closure reaches fails" \
  "$(sed 's/^crates\/ravel-types	.*$/crates\/ravel-types	sql,flight-sql,agents/' <<<"${GOOD}")" 1 \
  "crate ravel-types (crates/ravel-types): its closure reaches area mcp, which the map row omits"

# FLIP: skip the reverse pass over the map's rows and a row for a crate that
# no longer exists stays in the map: exit 0.
check_map "a row for no workspace crate fails" \
  "$(printf '%s\n%s\t%s' "${GOOD}" crates/gone -)" 1 \
  "map row crates/gone is not a workspace crate directory"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
