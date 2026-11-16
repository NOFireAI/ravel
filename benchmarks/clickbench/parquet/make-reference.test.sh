#!/usr/bin/env bash
# Cases for make-reference.sh, driven by a stub datafusion-cli on PATH. The
# stub answers --version from STUB_VERSION, logs every SQL file it is given,
# and fails any file containing STUB_FAIL_PATTERN. Nothing here needs the real
# datafusion-cli or the real hits.parquet, and every output goes under its
# own mktemp -d.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/make-reference.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() {
  echo "FAIL $1: $2"
  fails=$((fails + 1))
}

mkdir -p "$TMP/bin" "$TMP/data"
cat >"$TMP/bin/datafusion-cli" <<'STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then
  echo "$STUB_VERSION"
  exit 0
fi
file=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-f" ]]; then
    file="$2"
  fi
  shift
done
cat "$file" >>"$STUB_LOG"
echo "----" >>"$STUB_LOG"
if [[ -n "${STUB_FAIL_PATTERN:-}" ]] && grep -qF "$STUB_FAIL_PATTERN" "$file"; then
  echo "Error: stub refused" >&2
  exit 1
fi
echo '[{"count(*)":99997497}]'
STUB
chmod +x "$TMP/bin/datafusion-cli"
: >"$TMP/data/hits.parquet"

# run_case <name> <version> <fail-pattern>: runs the script into $TMP/<name>/ref
# and leaves its exit code in $TMP/<name>/rc.
run_case() {
  local name="$1" rc=0
  mkdir -p "$TMP/$name"
  PATH="$TMP/bin:$PATH" STUB_VERSION="$2" STUB_FAIL_PATTERN="$3" \
    STUB_LOG="$TMP/$name/stub.log" \
    bash "$SCRIPT" "$TMP/data/hits.parquet" "$TMP/$name/ref" \
    >"$TMP/$name/out" 2>"$TMP/$name/err" || rc=$?
  echo "$rc" >"$TMP/$name/rc"
}

# 1. A datafusion-cli other than 54.1.0 is refused before anything is written.
run_case wrong-version "datafusion-cli 54.0.0" ""
rc="$(<"$TMP/wrong-version/rc")"
if [[ "$rc" -ne 1 ]]; then
  fail wrong-version "exit $rc, want 1"
elif ! grep -qF "datafusion-cli 54.0.0" "$TMP/wrong-version/err"; then
  fail wrong-version "stderr does not name the version: $(<"$TMP/wrong-version/err")"
elif [[ -e "$TMP/wrong-version/ref/VERSION" || -e "$TMP/wrong-version/ref/q01.json" ]]; then
  fail wrong-version "wrote output despite refusing"
elif [[ -e "$TMP/wrong-version/stub.log" ]]; then
  fail wrong-version "ran a statement despite refusing"
else
  pass wrong-version
fi

# 2. 54.1.0 writes one file per statement, with LOCATION edited to the local
#    path, and VERSION last.
run_case written "datafusion-cli 54.1.0" ""
rc="$(<"$TMP/written/rc")"
ref="$TMP/written/ref"
if [[ "$rc" -ne 0 ]]; then
  fail written "exit $rc, want 0: $(<"$TMP/written/err")"
elif [[ "$(<"$ref/VERSION")" != "datafusion-cli 54.1.0" ]]; then
  fail written "VERSION holds '$(<"$ref/VERSION")'"
elif [[ "$(<"$ref/q01.json")" != '[{"count(*)":99997497}]' ]]; then
  fail written "q01.json holds '$(<"$ref/q01.json")'"
elif [[ ! -f "$ref/q43.json" || ! -f "$ref/describe.json" || -e "$ref/q44.json" ]]; then
  fail written "expected q01..q43 and describe.json only: $(ls "$ref")"
elif ! grep -qF "LOCATION '$TMP/data/hits.parquet'" "$TMP/written/stub.log"; then
  fail written "LOCATION was not pointed at the local file"
elif grep -qF "LOCATION 'hits.parquet'" "$TMP/written/stub.log"; then
  fail written "upstream LOCATION reached datafusion-cli"
elif ! grep -qF 'SELECT COUNT(*) FROM hits WHERE "AdvEngineID" <> 0;' "$TMP/written/stub.log"; then
  fail written "statement 2 never reached datafusion-cli"
else
  pass written
fi

# 3. A failing statement is named, its file is absent, and VERSION is not
#    written, so the bench refuses the directory.
run_case one-fails "datafusion-cli 54.1.0" 'WHERE "AdvEngineID" <> 0;'
rc="$(<"$TMP/one-fails/rc")"
ref="$TMP/one-fails/ref"
if [[ "$rc" -ne 1 ]]; then
  fail one-fails "exit $rc, want 1"
elif ! grep -qF "failed: q02 (exit 1): Error: stub refused" "$TMP/one-fails/err"; then
  fail one-fails "q02 not named: $(<"$TMP/one-fails/err")"
elif [[ -e "$ref/q02.json" || -e "$ref/VERSION" ]]; then
  fail one-fails "wrote q02.json or VERSION"
elif [[ ! -f "$ref/q03.json" ]]; then
  fail one-fails "later statements did not run"
else
  pass one-fails
fi

# 4. A rerun clears an earlier run's files first.
run_case rerun "datafusion-cli 54.1.0" ""
run_case rerun "datafusion-cli 54.1.0" 'WHERE "AdvEngineID" <> 0;'
if [[ -e "$TMP/rerun/ref/q02.json" || -e "$TMP/rerun/ref/VERSION" ]]; then
  fail rerun "a stale q02.json or VERSION survived the failed rerun"
else
  pass rerun
fi

# 5. Bad usage.
rc=0
bash "$SCRIPT" >/dev/null 2>&1 || rc=$?
if [[ "$rc" -ne 64 ]]; then
  fail no-args "exit $rc, want 64"
else
  pass no-args
fi
cp "$TMP/data/hits.parquet" "$TMP/data/it's.parquet"
rc=0
PATH="$TMP/bin:$PATH" STUB_VERSION="datafusion-cli 54.1.0" STUB_LOG="$TMP/quote.log" \
  bash "$SCRIPT" "$TMP/data/it's.parquet" "$TMP/quote-ref" >/dev/null 2>&1 || rc=$?
if [[ "$rc" -ne 64 ]]; then
  fail quoted-path "exit $rc, want 64"
else
  pass quoted-path
fi

if [[ $fails -ne 0 ]]; then
  echo "$fails case(s) failed"
  exit 1
fi
echo "all cases passed"
