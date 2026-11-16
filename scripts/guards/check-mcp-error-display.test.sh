#!/usr/bin/env bash
# Cases for check-mcp-error-display.sh. Add a case here before changing a
# rule, the way check-guarded-sql-parse.test.sh works for its guard.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout.
#
# Run: bash scripts/guards/check-mcp-error-display.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-mcp-error-display.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-mcp-error-display-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

SRC="services/ravel-server/src/mcp"
TOOLS="crates/ravel-mcp/src"

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed and one clean
# source file in each default root holding a fixed-message site, so the anchor
# count is never what a case measures unless it says so. Prints its path.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/${SRC}" "${dir}/${TOOLS}/tools"
  cp "${GUARD}" "${dir}/scripts/guards/check-mcp-error-display.sh"
  cat >"${dir}/${SRC}/clean.rs" <<'RS'
//! A clean module. Its prose names d4::internal(format!("{error}")) freely.
fn tenant() -> Result<TenantHash, McpError> {
    Err(McpError::internal_error("the request carries no authenticated tenant", None))
}
RS
  cat >"${dir}/${TOOLS}/tools/clean.rs" <<'RS'
fn fixed() -> Failure {
    internal("a fixed message".to_string())
}
RS
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty> [guard args...]
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  shift 4
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-mcp-error-display.sh "$@" 2>&1)" || rc=$?
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

# check_findings <name> <repo> <want-count>: the guard prints exactly that
# many findings.
check_findings() {
  local name="$1" dir="$2" want="$3"
  local out count
  out="$(cd "${dir}" && bash scripts/guards/check-mcp-error-display.sh 2>&1)"
  count="$(printf '%s\n' "${out}" | grep -c ': error-display: ')"
  if [[ "${count}" != "${want}" ]]; then
    printf 'FAIL  %s: %s findings, wanted %s\n' "${name}" "${count}" "${want}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- the clean shape -------------------------------------------------------

d="$(new_repo clean)"
cat >"${d}/${SRC}/fixed.rs" <<'RS'
fn change_point_cells(outcome: Result<R, AnalyticsError>, tenant_hash: TenantHash) -> Result<Row, Failure> {
    match outcome {
        Ok(result) => Ok(row(result)),
        Err(error) => Err(d4::redacted_internal(CHANGE_POINT_FAILED, &error, tenant_hash)),
    }
}
fn literal() -> Failure {
    d4::internal("a fixed message".to_string())
}
fn raw_literal() -> Failure {
    d4::internal(r#"a "fixed" message"#.to_string())
}
fn service(error: &ServiceError) -> Failure {
    d4::failure(error)
}
fn fixed_reason() -> McpError {
    McpError::internal_error("refused", Some(serde_json::json!({ "reason": "a fixed reason" })))
}
RS
check "passes fixed messages, literal to_string and a fixed reason" "${d}" 0 "no MCP failure is built from formatted error text"

# --- one planted leak per construct ----------------------------------------

d="$(new_repo internal_format)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn summary_row(error: AnalyticsError) -> Failure {
    d4::internal(format!("summary: {error}"))
}
RS
check "flags d4::internal built with format!" "${d}" 1 "leak.rs:2: error-display: the arguments of d4::internal("

d="$(new_repo failure_to_string)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn wrap(error: StoreError) -> Failure {
    d4::failure(&ServiceError::internal(error.to_string()))
}
RS
check "flags d4::failure built with to_string" "${d}" 1 "leak.rs:2: error-display: the arguments of d4::failure("

d="$(new_repo envelope_spelling)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn wrap(error: StoreError) -> Failure {
    super::envelope::internal(format!("{error:?}"))
}
RS
check "flags the envelope:: spelling of the same call" "${d}" 1 "leak.rs:2: error-display: the arguments of super::envelope::internal("

d="$(new_repo mcp_error_new)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn refuse(error: ToolError) -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, error.to_string(), None)
}
RS
check "flags McpError::new built with to_string" "${d}" 1 "leak.rs:2: error-display: the arguments of McpError::new("

d="$(new_repo json_reason)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn data(err: serde_json::Error) -> Value {
    serde_json::json!({ "reason": err.to_string() })
}
RS
check "flags a json! reason built with to_string" "${d}" 1 "leak.rs:2: error-display: the arguments of json!("

d="$(new_repo interpolated)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn fail(e: StoreError) -> Failure {
    d4::internal(compose!("the fetch failed: {e}"))
}
RS
check "flags an interpolated {e} without format!" "${d}" 1 "leak.rs:2: error-display:"

d="$(new_repo multiline)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn tool_result(envelope: &Envelope) -> Result<Value, McpError> {
    serde_json::to_value(envelope).map_err(|error| {
        McpError::internal_error(
            "the result envelope could not be serialized",
            Some(serde_json::json!({
                "reason": error.to_string()
            })),
        )
    })
}
RS
check "follows a call's arguments across lines to the anchor line" "${d}" 1 "leak.rs:3: error-display: the arguments of McpError::internal_error("
check "reports the nested json! at its own line" "${d}" 1 "leak.rs:5: error-display: the arguments of json!("

# --- every spelling of the failure constructors ----------------------------

d="$(new_repo redacted_qualified)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn wrap(error: StoreError, tenant_hash: TenantHash) -> Failure {
    d4::redacted_internal(&format!("the read failed: {error}"), &error, tenant_hash)
}
RS
check "flags d4::redacted_internal built with format!" "${d}" 1 "leak.rs:2: error-display: the arguments of d4::redacted_internal("

d="$(new_repo redacted_qualified_allowed)"
cat >"${d}/${SRC}/fixed.rs" <<'RS'
fn wrap(error: StoreError, tenant_hash: TenantHash) -> Failure {
    d4::redacted_internal(READ_FAILED, &error, tenant_hash)
}
fn shape(cell: &Cell, tenant_hash: TenantHash) -> Failure {
    // mcp-error-display-allow: names the cell's variant only.
    d4::redacted_internal(&format!("a {} cell", cell.kind()), &format_args!("{cell:?}"), tenant_hash)
}
RS
check "passes d4::redacted_internal with a fixed message or a marker" "${d}" 0 ""

d="$(new_repo redacted_bare)"
cat >"${d}/${TOOLS}/tools/leak.rs" <<'RS'
fn sample(err: serde_json::Error, tenant_hash: TenantHash) -> Failure {
    redacted_internal(&format!("a sample did not serialize: {err}"), &err, tenant_hash)
}
RS
check "flags an unqualified redacted_internal built with format!" "${d}" 1 "tools/leak.rs:2: error-display: the arguments of redacted_internal("

d="$(new_repo redacted_bare_allowed)"
cat >"${d}/${TOOLS}/tools/fixed.rs" <<'RS'
fn sample(err: serde_json::Error, tenant_hash: TenantHash) -> Failure {
    redacted_internal(SAMPLE_NOT_SERIALIZED, &err, tenant_hash)
}
RS
check "passes an unqualified redacted_internal with a fixed message" "${d}" 0 ""

d="$(new_repo internal_bare)"
cat >"${d}/${TOOLS}/tools/leak.rs" <<'RS'
fn sample(err: serde_json::Error) -> Failure {
    internal(format!("a PromQL sample did not serialize: {err}"))
}
RS
check "flags an unqualified internal built with format!" "${d}" 1 "tools/leak.rs:2: error-display: the arguments of internal("

d="$(new_repo internal_bare_allowed)"
cat >"${d}/${TOOLS}/tools/fixed.rs" <<'RS'
fn column_index(name: &str) -> Failure {
    // mcp-error-display-allow: every caller passes a compiled-in column name.
    internal(format!("{name} is not an analytics column"))
}
RS
check "passes an unqualified internal under a reasoned marker" "${d}" 0 ""

d="$(new_repo failure_bare)"
cat >"${d}/${TOOLS}/tools/leak.rs" <<'RS'
fn refuse(error: StoreError) -> Refusal {
    (failure(FailureClass::Internal, error.to_string()), Vec::new())
}
RS
check "flags an unqualified failure built with to_string" "${d}" 1 "tools/leak.rs:2: error-display: the arguments of failure("

d="$(new_repo failure_bare_allowed)"
cat >"${d}/${TOOLS}/tools/fixed.rs" <<'RS'
fn refuse(error: &ValidationError) -> Refusal {
    // mcp-error-display-allow: the refusal of the caller's own statement.
    (failure(FailureClass::Validation, error.to_string()), Vec::new())
}
RS
check "passes an unqualified failure under a reasoned marker" "${d}" 0 ""

d="$(new_repo method_and_longer_name)"
cat >"${d}/${TOOLS}/tools/data.rs" <<'RS'
fn refuse(error: CursorError, sql: SqlError) -> (Failure, Failure) {
    (error.failure(), sql_failure(&sql.to_string()))
}
RS
check "a .failure() method and a longer name ending in failure are not anchors" "${d}" 0 ""

d="$(new_repo qualified_once)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn summary_row(error: AnalyticsError) -> Failure {
    d4::internal(format!("summary: {error}"))
}
RS
check_findings "a qualified call is one finding, not one per path segment" "${d}" 1

# --- what is not a leak ----------------------------------------------------

d="$(new_repo json_no_reason)"
cat >"${d}/${SRC}/data.rs" <<'RS'
fn row(count: u64) -> Value {
    json!({ "count": count.to_string() })
}
RS
check "a json! without a reason key is not a failure payload" "${d}" 0 ""

d="$(new_repo paren_in_string)"
cat >"${d}/${SRC}/data.rs" <<'RS'
fn fixed(error: E) -> (Failure, String) {
    (d4::internal("an ( open paren".to_string()), format!("{error}"))
}
RS
check "a parenthesis in a string does not stretch the call past its close" "${d}" 0 ""

d="$(new_repo paren_in_string_leak)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn leak(error: E) -> Failure {
    d4::internal(concat("))", error.to_string()))
}
RS
check "a parenthesis in a string does not hide a later leak" "${d}" 1 "leak.rs:2: error-display:"

d="$(new_repo char_paren)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn leak<'a>(error: &'a E) -> Failure {
    d4::internal(join(')', ')', error.to_string()))
}
RS
check "a char literal paren and a lifetime do not blind the scan" "${d}" 1 "leak.rs:2: error-display:"

d="$(new_repo literal_span)"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn leak(error: E) -> Failure {
    d4::internal(join("a", error.to_string(), ".to_string()"))
}
RS
check "the literal exemption cannot span from one literal to the next" "${d}" 1 "leak.rs:2: error-display:"

d="$(new_repo escaped_quote)"
cat >"${d}/${SRC}/data.rs" <<'RS'
fn fixed(error: E) -> (Failure, String) {
    (d4::internal("say \"".to_string()), format!("{error}"))
}
RS
check "an escaped quote does not end a literal" "${d}" 0 ""

d="$(new_repo prose_in_string)"
cat >"${d}/${SRC}/data.rs" <<'RS'
fn fixed() -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, "no format! and no .to_string() here", None)
}
RS
check "a string naming format! or to_string is not a finding" "${d}" 0 ""

d="$(new_repo comment)"
cat >"${d}/${SRC}/data.rs" <<'RS'
// d4::internal(format!("{error}")) in a comment is not a call.
/* McpError::new(code, error.to_string(), None) in a block comment
   spanning two lines is not one either. */
fn noop() {}
RS
check "ignores calls in comments" "${d}" 0 ""

# --- the allow marker ------------------------------------------------------

d="$(new_repo marker_inline)"
cat >"${d}/${SRC}/echo.rs" <<'RS'
fn unknown(name: &str) -> McpError {
    McpError::new(ErrorCode::METHOD_NOT_FOUND, format!("unknown tool: {name}"), None) // mcp-error-display-allow: echoes the caller's tool name
}
RS
check "an inline marker with a reason suppresses" "${d}" 0 ""

d="$(new_repo marker_block)"
cat >"${d}/${SRC}/echo.rs" <<'RS'
fn route(error: ToolError) -> Result<(), McpError> {
    Err(
        // mcp-error-display-allow: the Display is a fixed sentence around
        // the tool name the caller sent.
        McpError::new(ErrorCode::METHOD_NOT_FOUND, error.to_string(), None),
    )
}
RS
check "a marker in the block directly above the call suppresses" "${d}" 0 ""
check "--list-allowed names the suppressed call" "${d}" 0 "echo.rs:5: allowed: McpError::new(" --list-allowed

d="$(new_repo marker_nested)"
cat >"${d}/${SRC}/echo.rs" <<'RS'
fn refuse(name: &str) -> McpError {
    // mcp-error-display-allow: the reason names the tool the caller sent.
    McpError::invalid_params(
        "unknown tool",
        Some(json!({ "reason": format!("no tool {name}") })),
    )
}
RS
check "a marker on a call covers the calls nested in its arguments" "${d}" 0 ""

d="$(new_repo marker_no_reason)"
cat >"${d}/${SRC}/echo.rs" <<'RS'
fn route(error: ToolError) -> McpError {
    // mcp-error-display-allow:
    McpError::new(ErrorCode::METHOD_NOT_FOUND, error.to_string(), None)
}
RS
check "a marker with an empty reason does not suppress" "${d}" 1 "echo.rs:3: error-display:"

d="$(new_repo marker_stale)"
cat >"${d}/${SRC}/echo.rs" <<'RS'
// mcp-error-display-allow: this reason belongs to the function below it.
fn documented() {}

fn later(error: E) -> Failure {
    d4::internal(format!("{error}"))
}
RS
check "a marker does not carry past its own comment block" "${d}" 1 "echo.rs:5: error-display:"

d="$(new_repo marker_in_string)"
cat >"${d}/${SRC}/echo.rs" <<'RS'
fn later(error: E) -> Failure {
    d4::internal(format!("mcp-error-display-allow: not a comment {error}"))
}
RS
check "the marker only counts from a comment, not a string" "${d}" 1 "echo.rs:2: error-display:"

# --- test modules ----------------------------------------------------------

d="$(new_repo test_module)"
cat >"${d}/${SRC}/tested.rs" <<'RS'
fn fixed() -> Failure {
    d4::internal("fixed".to_string())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(super) mod tests {
    // A planted leak is what a test builds.
    fn planted(error: E) -> Failure {
        d4::internal(format!("{error}"))
    }
}
RS
check "a site inside a cfg(test) mod tests is out of scope" "${d}" 0 ""

d="$(new_repo test_fn)"
cat >"${d}/${SRC}/tested.rs" <<'RS'
#[cfg(test)]
fn helper(error: E) -> Failure {
    d4::internal(format!("{error}"))
}

fn production(error: E) -> Failure {
    d4::internal(format!("{error}"))
}
RS
check "a cfg(test) on anything but mod tests skips nothing" "${d}" 1 "tested.rs:3: error-display:"
check "and the code after it is still scanned" "${d}" 1 "tested.rs:7: error-display:"

d="$(new_repo test_module_per_file)"
cat >"${d}/${SRC}/a_tested.rs" <<'RS'
#[cfg(test)]
mod tests {}
RS
cat >"${d}/${SRC}/b_leak.rs" <<'RS'
fn leak(error: E) -> Failure {
    d4::internal(format!("{error}"))
}
RS
check "a test module ends the scan of its own file only" "${d}" 1 "b_leak.rs:2: error-display:"

# --- a scan that cannot mean anything --------------------------------------

d="$(new_repo moved)"
rm -rf "${d:?}/${SRC}"
mkdir -p "${d}/services/ravel-server/src/mcp_v2"
cp "${HERE}/check-mcp-error-display.sh" "${d}/scripts/guards/check-mcp-error-display.sh"
check "refuses when the MCP source directory has moved" "${d}" 2 "is gone"

d="$(new_repo empty)"
rm -f "${d}/${SRC}"/*.rs
check "refuses when the source directory holds no Rust sources" "${d}" 2 "no Rust sources"

d="$(new_repo no_anchors)"
cat >"${d}/${SRC}/clean.rs" <<'RS'
fn renamed(error: E) -> Fault {
    faults::raise(format!("{error}"))
}
RS
check "refuses when the sources hold none of its anchors" "${d}" 2 "cannot refuse anything"

d="$(new_repo tools_moved)"
rm -rf "${d:?}/${TOOLS}"
cat >"${d}/${SRC}/leak.rs" <<'RS'
fn summary_row(error: AnalyticsError) -> Failure {
    d4::internal(format!("summary: {error}"))
}
RS
check "refuses when the ravel-mcp source directory has moved" "${d}" 2 "crates/ravel-mcp/src is gone"
check "and still scans and reports the root that is there" "${d}" 2 "mcp/leak.rs:2: error-display:"

d="$(new_repo tools_empty)"
rm -f "${d}/${TOOLS}/tools"/*.rs
check "refuses when the ravel-mcp root holds no Rust sources" "${d}" 2 "no Rust sources under crates/ravel-mcp/src"

d="$(new_repo tools_no_anchors)"
cat >"${d}/${TOOLS}/tools/clean.rs" <<'RS'
fn renamed(error: E) -> Fault {
    faults::raise(format!("{error}"))
}
RS
check "refuses when the ravel-mcp root holds none of its anchors" "${d}" 2 "under crates/ravel-mcp/src, so this scan cannot refuse anything"

d="$(new_repo one_root_blind)"
cat >"${d}/${SRC}/clean.rs" <<'RS'
fn renamed(error: E) -> Fault {
    faults::raise(format!("{error}"))
}
RS
cat >"${d}/${TOOLS}/tools/leak.rs" <<'RS'
fn sample(err: serde_json::Error) -> Failure {
    internal(format!("{err}"))
}
RS
check "a blind root exits 2 even when the other root has anchors" "${d}" 2 "under services/ravel-server/src/mcp, so this scan cannot refuse anything"
check "and the other root's findings are still printed" "${d}" 2 "tools/leak.rs:2: error-display:"

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
check "an unknown option is a usage error" "${d}" 64 "unknown option" --nope
check "a named path that does not exist is a usage error" "${d}" 64 "no such directory" services/nope

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
