#!/usr/bin/env bash
# Executable part of the CodeRabbit acceptance matrix (ADR-0090).
#
# Covers every case in docs/guides/coderabbit-runbook.md that can be decided
# without a GitHub account, a CodeRabbit credential, or an administrator
# console. The cases it cannot cover are the ones that live in GitHub and
# CodeRabbit administration; those are listed at the end of a run with a pointer
# to the manual evidence steps, so a green run is never mistaken for a complete
# one.
#
# The authorization test executes the shell that actually ships in the workflow,
# extracted from the workflow file, against a mock `gh`. A copy of the logic
# would drift; the shipped copy cannot.
#
# Usage:
#   scripts/coderabbit-acceptance-tests.sh            # offline
#   VERIFY_DOWNLOAD=1 scripts/coderabbit-acceptance-tests.sh   # also fetch and
#                                                              # verify the
#                                                              # pinned CLI

set -Eeuo pipefail
umask 077

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

readonly WORKFLOW=".github/workflows/coderabbit-maintainer-review.yml"
readonly POLICY_DIR=".github/coderabbit"

passes=0
failures=0
tmp_root=$(mktemp -d)
trap 'rm -rf "$tmp_root"' EXIT

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256; }
else
  echo "neither sha256sum nor shasum is on PATH" >&2
  exit 1
fi

pass() { printf '  ok    %s\n' "$1"; passes=$((passes + 1)); }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }
check() { if [ "$1" = "0" ]; then pass "$2"; else fail "$2"; fi; }
section() { printf '\n%s\n' "$1"; }

# ---------------------------------------------------------------------------
section "A. The workflow has no trigger a non-maintainer can reach (cases 1-6)"
# ---------------------------------------------------------------------------
triggers=$(sed -n '/^on:/,/^permissions:/p' "$WORKFLOW")
rc=0; printf '%s' "$triggers" | grep -q 'workflow_dispatch:' || rc=1
check "$rc" "workflow_dispatch is declared"
for forbidden in pull_request pull_request_target issue_comment pull_request_review workflow_run repository_dispatch schedule push; do
  rc=0; printf '%s' "$triggers" | grep -Eq "^  ${forbidden}:" && rc=1
  check "$rc" "no ${forbidden} trigger"
done

# ---------------------------------------------------------------------------
section "B. Least privilege and no execution of pull-request content (case 16)"
# ---------------------------------------------------------------------------
rc=0; grep -q '^permissions: {}' "$WORKFLOW" || rc=1
check "$rc" "top-level permissions deny everything by default"
rc=0; grep -q 'environment: coderabbit-oss' "$WORKFLOW" || rc=1
check "$rc" "the review job runs in the coderabbit-oss environment"
rc=0; grep -q 'runs-on: ubuntu-24.04' "$WORKFLOW" || rc=1
check "$rc" "runners are GitHub-hosted and version-pinned"
rc=0; grep -q 'self-hosted' "$WORKFLOW" && rc=1
check "$rc" "no self-hosted runner"
# Comments and summary prose in this workflow legitimately name the things it
# refuses to run, so the forbidden-command checks look at executable shell only:
# comment lines and `echo` lines are stripped, and a command must appear in
# command position rather than anywhere in a line.
grep -vE '^[[:space:]]*#' "$WORKFLOW" | grep -vE '^[[:space:]]*echo ' > "${tmp_root}/exec-only.sh"
for forbidden in cargo 'scripts/gates\.sh' make npm yarn pnpm 'docker' kubectl rustc; do
  rc=0; grep -Eq "(^|[;&|(]|[[:space:]])${forbidden}([[:space:]]|$)" "${tmp_root}/exec-only.sh" && rc=1
  check "$rc" "workflow never runs ${forbidden}"
done
for forbidden in upload-artifact actions/cache; do
  rc=0; grep -Fq "$forbidden" "${tmp_root}/exec-only.sh" && rc=1
  check "$rc" "workflow never uses ${forbidden}"
done
rc=0; grep -q 'persist-credentials: false' "$WORKFLOW" || rc=1
check "$rc" "checkout does not persist credentials"
rc=0; grep -q 'core.hooksPath /dev/null' "$WORKFLOW" || rc=1
check "$rc" "git hooks are disabled on the pull-request tree"

# ---------------------------------------------------------------------------
section "C. Supply chain: every action pinned to a commit SHA"
# ---------------------------------------------------------------------------
unpinned=$(grep -oE 'uses: [^ ]+' "$WORKFLOW" | grep -vE 'uses: [^@]+@[0-9a-f]{40}$' || true)
rc=0; [ -z "$unpinned" ] || { rc=1; printf '    unpinned: %s\n' "$unpinned"; }
check "$rc" "no action is referenced by a mutable tag"
rc=0; grep -q 'CODERABBIT_CLI_DISABLE_AUTO_UPDATE' "$WORKFLOW" || rc=1
check "$rc" "the CLI cannot self-update in CI"
rc=0; grep -q 'sha256sum -c' "$WORKFLOW" || rc=1
check "$rc" "the CLI download is checksum-verified before execution"
rc=0; grep -q 'install.sh' "$WORKFLOW" && rc=1
check "$rc" "no piped installer script"
# shellcheck disable=SC1091
. "${POLICY_DIR}/cli-pin.env"
for digest in "$CODERABBIT_CLI_SHA256_LINUX_X64" "$CODERABBIT_CLI_SHA256_LINUX_ARM64" "$CODERABBIT_CLI_SHA256_DARWIN_ARM64"; do
  rc=0; [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || rc=1
  check "$rc" "pinned digest ${digest:0:12}... is a SHA-256"
done

# ---------------------------------------------------------------------------
section "D. Credential exposure (case 21)"
# ---------------------------------------------------------------------------
secret_refs=$(grep -c 'secrets.CODERABBIT_API_KEY' "$WORKFLOW" || true)
rc=0; [ "$secret_refs" = "1" ] || rc=1
check "$rc" "the secret is referenced by exactly one step (found ${secret_refs})"
rc=0; grep -A3 'CODERABBIT_API_KEY: ' "$WORKFLOW" | grep -q 'GITHUB_OUTPUT' && rc=1
check "$rc" "the secret never reaches a step output"
rc=0; grep -q 'rm -rf "${HOME}/.coderabbit"' "$WORKFLOW" || rc=1
check "$rc" "CLI state is scrubbed from the runner"

# ---------------------------------------------------------------------------
section "E. Authorization gate, running the shipped shell (cases 1-8)"
# ---------------------------------------------------------------------------
mock_bin="${tmp_root}/bin"
mkdir -p "$mock_bin"
cat > "${mock_bin}/gh" <<'MOCK'
#!/usr/bin/env bash
target=""
for a in "$@"; do case "$a" in *collaborators/*) target="$a";; esac; done
login="${target#*collaborators/}"; login="${login%/permission}"
case "$login" in
  admin-user)    echo '{"role_name":"admin","permission":"admin"}' ;;
  maintain-user) echo '{"role_name":"maintain","permission":"write"}' ;;
  write-user)    echo '{"role_name":"write","permission":"write"}' ;;
  triage-user)   echo '{"role_name":"triage","permission":"read"}' ;;
  read-user)     echo '{"role_name":"read","permission":"read"}' ;;
  none-user)     echo '{"role_name":"none","permission":"none"}' ;;
  no-role-field) echo '{"permission":"write"}' ;;
  api-failure)   exit 1 ;;
  *)             echo '{}' ;;
esac
MOCK
chmod +x "${mock_bin}/gh"

python3 "${POLICY_DIR}/extract-run-block.py" "$WORKFLOW" \
  "Verify actor holds maintain or admin" > "${tmp_root}/authorize.sh"

run_gate() {
  ( cd "$tmp_root" \
    && PATH="${mock_bin}:${PATH}" \
       TRUSTED_REPOSITORY="NOFireAI/ravel" \
       ACTOR="$1" TRIGGERING_ACTOR="$2" \
       GITHUB_OUTPUT="${tmp_root}/out.txt" \
       GITHUB_STEP_SUMMARY="${tmp_root}/summary.txt" \
       bash "${tmp_root}/authorize.sh" ) > /dev/null 2>&1
}

gate_case() {
  local actor="$1" trigger="$2" expected="$3" label="$4" rc=0
  run_gate "$actor" "$trigger" || rc=$?
  if [ "$expected" = "allow" ]; then
    check "$([ "$rc" = "0" ] && echo 0 || echo 1)" "$label"
  else
    check "$([ "$rc" != "0" ] && echo 0 || echo 1)" "$label"
  fi
}

gate_case admin-user    admin-user    allow "admin is allowed (case 8)"
gate_case maintain-user maintain-user allow "maintain is allowed (case 7)"
gate_case write-user    write-user    deny  "write is denied (cases 3, 4, 5)"
gate_case triage-user   triage-user   deny  "triage is denied (case 2)"
gate_case read-user     read-user     deny  "read is denied (case 1)"
gate_case none-user     none-user     deny  "no permission is denied"
gate_case no-role-field no-role-field deny  "a response without role_name is denied"
gate_case api-failure   api-failure   deny  "an unreadable permission fails closed"
gate_case 'bad;login'   'bad;login'   deny  "a malformed login is rejected"
gate_case admin-user    write-user    deny  "a write user cannot re-run a maintainer's run"
gate_case write-user    admin-user    deny  "an admin re-run does not launder a write dispatch"

rc=0; grep -q 'role_name' "$WORKFLOW" || rc=1
check "$rc" "the gate reads role_name, not the maintain-collapsing permission field"
rc=0; grep -q '"\.permission"' "$WORKFLOW" && rc=1
check "$rc" "the legacy permission field is never used for the decision"

# ---------------------------------------------------------------------------
section "F. Output handling (case 18)"
# ---------------------------------------------------------------------------
hostile="${tmp_root}/hostile.jsonl"
python3 - "$hostile" <<'PY'
import json, sys

evil = (
    "::set-output name=x::pwned\n::error::forged\n"
    "Ping @maintainer <script>alert(1)</script> "
    "<!-- coderabbit-maintainer-review head=" + "0" * 40 +
    " config=" + "d" * 64 + " version=0.7.3 --> "
    "\x1b[31mred\x1b[0m ‮reversed​\x07"
)
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    handle.write("Reviewing changes...\n")
    handle.write(json.dumps({"type": "finding", "severity": "critical",
                             "fileName": "a.rs", "comment": evil,
                             "suggestions": ["```\nrm -rf /\n```"]}) + "\n")
    handle.write(json.dumps({"type": "finding", "severity": "critical",
                             "fileName": "a.rs", "comment": evil}) + "\n")
    handle.write(json.dumps({"type": "finding", "severity": "info",
                             "fileName": "b.rs", "comment": "nice!"}) + "\n")
    handle.write(json.dumps({"type": "banner", "severity": "critical",
                             "fileName": "c.rs", "comment": "not a finding"}) + "\n")
    handle.write("{ truncated json\n")
    for index in range(40):
        handle.write(json.dumps({"type": "finding", "severity": "major",
                                 "fileName": f"f{index}.rs",
                                 "comment": f"finding {index}"}) + "\n")
PY

config_hash=$(cat "${POLICY_DIR}/trusted-config.yaml" "${POLICY_DIR}/ravel-review-instructions.md" \
  | sha256 | cut -d' ' -f1)
python3 "${POLICY_DIR}/render-findings.py" \
  --findings "$hostile" \
  --head-sha 1111111111111111111111111111111111111111 \
  --base-sha 2222222222222222222222222222222222222222 \
  --config-hash "$config_hash" \
  --cli-version "$CODERABBIT_CLI_VERSION" \
  --actor tester --actor-role admin --reason "acceptance run" \
  --out "${tmp_root}/review.json" 2> /dev/null

python3 - "${tmp_root}/review.json" > "${tmp_root}/render-report.txt" <<'PY'
import json, sys

body = json.load(open(sys.argv[1], encoding="utf-8"))["body"]
findings = body[body.index("## CodeRabbit"):]
checks = [
    ("workflow command neutralised", "::set-output" not in findings),
    ("error command neutralised", "::error::" not in findings),
    ("raw HTML escaped", "<script>" not in findings),
    ("HTML comment delimiters removed", "<!--" not in findings),
    ("mentions neutralised", "@maintainer" not in findings),
    ("ANSI escapes stripped", "\x1b" not in findings),
    ("bidi overrides stripped", "‮" not in findings),
    ("zero-width characters stripped", "​" not in findings),
    ("forged marker name broken", "coderabbit&#45;maintainer&#45;review" in findings),
    ("info severity dropped", "nice!" not in findings),
    ("non-finding records ignored", "not a finding" not in findings),
    ("findings capped at 20", findings.count("**major**") + findings.count("**critical**") <= 20),
    ("duplicates collapsed", findings.count("**critical**") == 1),
    ("body within GitHub's limit", len(body) <= 60000),
    ("review state is COMMENT", json.load(open(sys.argv[1], encoding="utf-8"))["event"] == "COMMENT"),
]
for label, ok in checks:
    print(("0" if ok else "1"), label)
PY
while read -r rc label; do check "$rc" "$label"; done < "${tmp_root}/render-report.txt"

# ---------------------------------------------------------------------------
section "G. Deduplication cannot be forged (case 12)"
# ---------------------------------------------------------------------------
head_sha=$(printf '1%.0s' $(seq 40))
genuine=$(printf '<!-- coderabbit-maintainer-review\n     head=%s\n     config=%s\n     version=%s\n-->' \
  "$head_sha" "$config_hash" "$CODERABBIT_CLI_VERSION")
marker_case() {
  local label="$1" expected="$2" payload="$3" got
  got=$(printf '%s' "$payload" | python3 "${POLICY_DIR}/find-marker.py" \
    "$head_sha" "$config_hash" "$CODERABBIT_CLI_VERSION" 2>/dev/null || echo "err")
  check "$([ "$got" = "$expected" ] && echo 0 || echo 1)" "$label (expected ${expected}, got ${got})"
}
marker_case "a genuine marker is found" 1 "$(python3 -c 'import json,sys;print(json.dumps([sys.stdin.read()]))' <<< "$genuine")"
marker_case "a plaintext forgery is not a marker" 0 \
  "$(python3 -c "import json;print(json.dumps(['coderabbit-maintainer-review head=${head_sha} config=${config_hash} version=${CODERABBIT_CLI_VERSION}']))")"
marker_case "fields split across two reviews are not a marker" 0 \
  "$(python3 -c "import json;print(json.dumps(['<!-- coderabbit-maintainer-review head=${head_sha}', 'config=${config_hash} version=${CODERABBIT_CLI_VERSION} -->']))")"
marker_case "no reviews means no marker" 0 "[]"
marker_case "malformed input fails closed" err "not json"

# ---------------------------------------------------------------------------
section "H. Trusted policy disables every mutating capability (case 15, 17)"
# ---------------------------------------------------------------------------
policy_disabled() {
  local file="$1" key="$2" rc=0
  grep -A1 -E "^ *${key}:" "$file" | grep -q 'enabled: false' || rc=1
  check "$rc" "$(basename "$file"): ${key} disabled"
}
for file in "${POLICY_DIR}/trusted-config.yaml" ".coderabbit.yaml"; do
  for key in autofix fix_ci unit_tests docstrings resolve_merge_conflict simplify; do
    policy_disabled "$file" "$key"
  done
  rc=0; grep -A2 -E '^ *auto_review:' "$file" | grep -q 'enabled: false' || rc=1
  check "$rc" "$(basename "$file"): automatic review disabled"
  rc=0; grep -q 'auto_incremental_review: false' "$file" || rc=1
  check "$rc" "$(basename "$file"): automatic incremental review disabled"
  rc=0; grep -q 'auto_reply: false' "$file" || rc=1
  check "$rc" "$(basename "$file"): chat auto-reply disabled"
  rc=0; grep -q 'allow_non_org_members: false' "$file" || rc=1
  check "$rc" "$(basename "$file"): non-organization-member chat disabled"
  rc=0; grep -q 'opt_out: true' "$file" || rc=1
  check "$rc" "$(basename "$file"): knowledge base opted out"
  rc=0; grep -A2 -E '^ *planning:' "$file" | grep -q 'enabled: false' || rc=1
  check "$rc" "$(basename "$file"): issue planning disabled"
done
rc=0; grep -q -- '--config' "$WORKFLOW" || rc=1
check "$rc" "the CLI is invoked with an explicit trusted configuration"
rc=0; grep -q 'RUNNER_TEMP}/coderabbit-trusted' "$WORKFLOW" || rc=1
check "$rc" "the trusted policy is staged outside every pull-request tree"

# ---------------------------------------------------------------------------
section "I. Cost controls (cases 19, 20)"
# ---------------------------------------------------------------------------
rc=0; grep -q 'cancel-in-progress: false' "$WORKFLOW" || rc=1
check "$rc" "concurrency does not cancel a run that may have already spent a review"
rc=0; grep -q 'group: coderabbit-maintainer-review-${{ github.repository }}-${{ inputs.pr_number }}' "$WORKFLOW" || rc=1
check "$rc" "concurrency is keyed on repository and pull-request number"
invocations=$(grep -c '"\$CLI_BIN" review' "$WORKFLOW" || true)
rc=0; [ "$invocations" = "1" ] || rc=1
check "$rc" "exactly one CodeRabbit invocation in the workflow (found ${invocations})"
rc=0; grep -q 'rate limit exceeded|too many files|payload_too_large' "$WORKFLOW" || rc=1
check "$rc" "a rate-limited or over-limit response is detected"
rc=0; grep -Eq 'strategy:|matrix:' "$WORKFLOW" && rc=1
check "$rc" "no matrix that would multiply reviews"
# Scoped to the step that spends the allowance. Elsewhere in the workflow a
# loop is fine: the authorization step iterates over two logins.
python3 "${POLICY_DIR}/extract-run-block.py" "$WORKFLOW" "Run CodeRabbit" > "${tmp_root}/run-step.sh"
rc=0; grep -Eq '(^|[;&|( ])(until|while|for)[ (]|--retry' "${tmp_root}/run-step.sh" && rc=1
check "$rc" "no loop or retry in the step that calls CodeRabbit"
calls=$(grep -c '"\$CLI_BIN" review' "${tmp_root}/run-step.sh" || true)
rc=0; [ "$calls" = "1" ] || rc=1
check "$rc" "that step calls CodeRabbit exactly once (found ${calls})"

# ---------------------------------------------------------------------------
section "J. Local fallback holds no shared credential"
# ---------------------------------------------------------------------------
fallback="scripts/coderabbit-maintainer-review.sh"
rc=0; grep -q 'secrets\.' "$fallback" && rc=1
check "$rc" "the fallback references no repository secret"
rc=0; grep -q 'CODERABBIT_API_KEY' "$fallback" && rc=1
check "$rc" "the fallback never reads a shared API key"
rc=0; grep -q 'role_name' "$fallback" || rc=1
check "$rc" "the fallback verifies role_name"
rc=0; grep -q 'publish' "$fallback" || rc=1
check "$rc" "the fallback requires an explicit --publish to post"

# ---------------------------------------------------------------------------
if [ "${VERIFY_DOWNLOAD:-0}" = "1" ]; then
section "K. The pinned CLI artifact matches its recorded digest"
  zip="${tmp_root}/coderabbit-linux-x64.zip"
  rc=0
  curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 --max-time 300 \
    -o "$zip" "https://cli.coderabbit.ai/releases/${CODERABBIT_CLI_VERSION}/coderabbit-linux-x64.zip" || rc=1
  check "$rc" "the pinned artifact downloads"
  if [ "$rc" = "0" ]; then
    got=$(sha256 < "$zip" | cut -d' ' -f1)
    rc=0; [ "$got" = "$CODERABBIT_CLI_SHA256_LINUX_X64" ] || rc=1
    check "$rc" "linux-x64 digest matches the pin"
  fi
fi

# ---------------------------------------------------------------------------
section "Cases this suite cannot decide"
# ---------------------------------------------------------------------------
cat <<'NOTE'
  These need a GitHub or CodeRabbit console and named accounts. Follow the
  manual evidence steps in docs/guides/coderabbit-runbook.md:

    - CodeRabbit plan is Open Source, add-on disabled, no payment method,
      Agentic API key issuable at no cost (runbook step 1)
    - GitHub App restrictions, tested from read/triage/write/outside/fork
      identities (runbook step 2, acceptance case 6)
    - Environment reviewers, self-review prevention, and the main-only
      deployment branch policy (runbook step 3, acceptance case 22)
    - protect-main requires a code-owner approval (runbook step 5,
      acceptance cases 14 and 15)
    - No credit spend after a rate limit or an oversized pull request
      (acceptance cases 19 and 20, CodeRabbit usage page)
NOTE

printf '\n%s passed, %s failed\n' "$passes" "$failures"
[ "$failures" -eq 0 ]
