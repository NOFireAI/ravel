#!/usr/bin/env bash
# Cases for brig.sh. Run:
#   bash scripts/brig.test.sh
#
# Every case runs a copy of brig.sh inside a throwaway checkout under $TMP,
# whose origin is a local fixture repository, against a stub brig that logs
# its arguments. git only, no network, no real brig.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/brig-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT
TMP="$(cd "${TMP}" && pwd -P)"

passes=0
fails=0
ok() { printf 'ok    %s\n' "$1"; passes=$((passes + 1)); }
bad() { printf 'FAIL  %-52s %s\n' "$1" "$2"; fails=$((fails + 1)); }

git_q() { git -c init.defaultBranch=main -c user.email=t@example.test -c user.name=t "$@" >/dev/null 2>&1; }

# The stub: `run --help` lists --target unless STUB_OLD is set, `rm` exits
# with STUB_RM_EXIT, and every call appends its arguments and the
# environment brig.sh sets to STUB_LOG.
stub="${TMP}/bin/brig"
mkdir -p "${TMP}/bin"
cat > "${stub}" <<'EOF'
#!/bin/sh
echo "$* | hv=${BRIG_HYPERVISOR:-} pull=${BRIG_PULL:-}" >> "$STUB_LOG"
case "$1 ${2:-}" in
  'version '*) echo "brig v0.0.0-stub" ;;
  'run --help') [ -n "${STUB_OLD:-}" ] || echo '  --target string' ;;
  'rm '*) exit "${STUB_RM_EXIT:-0}" ;;
esac
exit 0
EOF
chmod +x "${stub}"

upstream="${TMP}/upstream"
git_q init "${upstream}"
echo 'apiVersion: brig.sh/v1alpha1' > "${upstream}/Brigfile"
git_q -C "${upstream}" add Brigfile
git_q -C "${upstream}" commit -m init

host="${TMP}/host"
git_q clone "${upstream}" "${host}"
mkdir -p "${host}/scripts"
cp "${HERE}/brig.sh" "${host}/scripts/brig.sh"

clones="${TMP}/clones"
log="${TMP}/stub.log"

# brig_sh <args...>: runs the host's copy; sets $code, $out (stdout+stderr).
brig_sh() {
  : > "${log}"
  out=$(cd "${host}" && env -u BRIG_HYPERVISOR -u BRIG_PULL \
    BRIG="${stub}" RAVEL_BRIG_CLONES="${clones}" STUB_LOG="${log}" \
    scripts/brig.sh "$@" 2>&1)
  code=$?
}

expect_code() { # label want
  if [[ "${code}" == "$2" ]]; then ok "$1"; else bad "$1" "want exit $2, got ${code}: ${out}"; fi
}
expect_out() { # label needle
  if [[ "${out}" == *"$2"* ]]; then ok "$1"; else bad "$1" "missing '$2' in: ${out}"; fi
}
expect_log() { # label needle
  if grep -qF -- "$2" "${log}"; then ok "$1"; else bad "$1" "missing '$2' in log: $(cat "${log}")"; fi
}
expect_no_log() { # label needle
  if grep -qF -- "$2" "${log}"; then bad "$1" "unexpected '$2' in log: $(cat "${log}")"; else ok "$1"; fi
}

# --- usage and names ---------------------------------------------------------
brig_sh;                expect_code "no command is usage" 64
brig_sh bogus t1;       expect_code "unknown command is usage" 64
for name in ../evil -x .hidden a/b ''; do
  brig_sh new "${name}"; expect_code "new refuses NAME '${name}'" 1
done

# --- new -------------------------------------------------------------------
brig_sh new t1;         expect_code "new makes a clone" 0
clone="${clones}/t1"
if [[ "$(git -C "${clone}" branch --show-current 2>/dev/null)" == t1 ]]; then ok "clone is on branch NAME"; else bad "clone is on branch NAME" "$(git -C "${clone}" branch --show-current 2>&1)"; fi
if [[ "$(git -C "${clone}" config --local --bool ravel.sandboxCheckout 2>/dev/null)" == true ]]; then ok "clone carries the sandbox mark"; else bad "clone carries the sandbox mark" "no mark"; fi
if [[ ! -e "${clone}/.git/objects/info/alternates" ]]; then ok "clone shares no objects with the host"; else bad "clone shares no objects with the host" "alternates present"; fi
brig_sh new t1;         expect_code "new refuses an existing clone" 1

# --- path and missing clones -------------------------------------------------
brig_sh path t1;        expect_code "path of a clone" 0
if [[ "${out}" == "${clone}" ]]; then ok "path prints the clone"; else bad "path prints the clone" "${out}"; fi
brig_sh plan nosuch;    expect_code "plan refuses a missing clone" 1
git_q clone "${upstream}" "${clones}/unmarked"
brig_sh plan unmarked;  expect_code "plan refuses an unmarked clone" 1
expect_out "unmarked clone is named as such" "is not a sandbox clone"

# --- brig version ------------------------------------------------------------
STUB_OLD=1 brig_sh plan t1
expect_code "a brig without targets is refused" 1
expect_out "the refusal names the main cask" "brew install --cask brig-sh/brig/brig@main"
expect_no_log "nothing runs on an old brig" "plan --target"
out=$(cd "${host}" && BRIG="${TMP}/bin/none" RAVEL_BRIG_CLONES="${clones}" scripts/brig.sh plan t1 2>&1); code=$?
expect_code "a missing brig is refused" 1
expect_out "the missing-brig refusal names the main cask" "brew install --cask brig-sh/brig/brig@main"

# --- the commands brig gets --------------------------------------------------
want_hv=''
[[ "$(uname -s)" == Darwin ]] && want_hv=hvi
brig_sh plan t1;        expect_code "plan runs" 0
expect_log "plan names the clone's Brigfile" "plan --target dev -f ${clone}/Brigfile"
expect_log "the hypervisor is hvi on macOS only" "hv=${want_hv} "
brig_sh run t1;         expect_code "run runs" 0
expect_log "run keeps the target's own args" "run --target dev -f ${clone}/Brigfile |"
brig_sh run t1 --continue
expect_log "run passes claude args through the guest script" "-- scripts/brig-dev.sh --continue"
brig_sh shell t1 -c 'make check'
expect_log "shell starts the guest script's shell" "-- scripts/brig-dev.sh --shell -c make check"
IMAGE=ghcr.io/x/y:sha-1 brig_sh plan t1
expect_log "IMAGE becomes --image" "--image=ghcr.io/x/y:sha-1"
brig_sh stop t1;        expect_log "stop names the clone" "stop --target dev -f ${clone}/Brigfile"

# --- update ------------------------------------------------------------------
STUB_RM_EXIT=3 brig_sh update t1
expect_code "update runs when there is no sandbox to remove" 0
expect_log "update pulls again" "pull=always"
STUB_RM_EXIT=5 brig_sh update t1
expect_code "update stops on a failed rm" 5
expect_no_log "nothing runs after a failed rm" "run --target"

# --- the trust boundary ------------------------------------------------------
mkdir -p "${clone}/scripts"
cp "${HERE}/brig.sh" "${clone}/scripts/brig.sh"
: > "${log}"
out=$(cd "${clone}" && BRIG="${stub}" RAVEL_BRIG_CLONES="${clones}" STUB_LOG="${log}" scripts/brig.sh plan t1 2>&1); code=$?
expect_code "a copy inside a sandbox clone refuses" 1
expect_out "the refusal names the sandbox clone" "is a sandbox clone"
expect_no_log "nothing runs from inside a sandbox clone" "plan"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
