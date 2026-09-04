#!/usr/bin/env bash
# Proofs for issue #1192: check-tla.sh must enforce its TLC wall-clock
# budget via GNU timeout(1), never report a timed-out run as a pass, and
# refuse outright when no GNU timeout is available. Run:
#   bash scripts/check-tla.test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/check-tla.sh"

pass=0
fail=0

ok()   { pass=$((pass + 1)); }
bad()  { printf 'FAIL  %s\n' "$1"; fail=$((fail + 1)); }

# --- (a) timeout fires and nothing survives ---------------------------------
# Source check-tla.sh's function definitions (everything but its unconditional
# `main "$@"` call on the last line) so run_tlc can be driven directly,
# against a throwaway FORMAL_DIR/CACHE_DIR, without needing the real TLC jar
# or network access.
source_lib() {
    local tmp_src
    tmp_src="$(mktemp)"
    sed '$d' "$SCRIPT" > "$tmp_src"
    # shellcheck disable=SC1090
    source "$tmp_src"
    rm -f "$tmp_src"
}
source_lib

run_tlc_case() {
    # run_tlc_case <shim-body-file> <budget> -> sets CASE_CODE, CASE_ELAPSED,
    # CASE_SHIM (the shim path, for the post-mortem pgrep check).
    local shim_body="$1" budget="$2"
    local tmp area_dir shim logfile start end
    tmp="$(mktemp -d)"
    area_dir="$tmp/area"
    mkdir -p "$area_dir"
    : > "$area_dir/smoke.cfg"
    shim="$tmp/java"
    cp "$shim_body" "$shim"
    chmod +x "$shim"

    FORMAL_DIR="$tmp"
    CACHE_DIR="$tmp/cache"
    mkdir -p "$CACHE_DIR"
    JAVA="$shim"
    JAR="/dev/null"
    resolve_timeout

    logfile="$tmp/tlc.log"
    start=$(date +%s)
    run_tlc area module "$area_dir/smoke.cfg" "$budget" "$logfile"
    CASE_CODE=$?
    end=$(date +%s)
    CASE_ELAPSED=$((end - start))
    CASE_SHIM="$shim"
    CASE_TMP="$tmp"
}

shim_sleeps_60="$(mktemp)"
cat > "$shim_sleeps_60" <<'EOF'
#!/usr/bin/env bash
exec sleep 60
EOF

shim_ignores_term="$(mktemp)"
cat > "$shim_ignores_term" <<'EOF'
#!/usr/bin/env bash
trap '' TERM
while :; do :; done
EOF

echo "--- (a1) budget 2s, shim obeys TERM: expect 124 in ~2s, nothing survives"
run_tlc_case "$shim_sleeps_60" 2
echo "    measured: exit=$CASE_CODE elapsed=${CASE_ELAPSED}s"
if [ "$CASE_CODE" -eq 124 ]; then ok; else bad "a1: expected exit 124, got $CASE_CODE"; fi
if [ "$CASE_ELAPSED" -ge 1 ] && [ "$CASE_ELAPSED" -le 8 ]; then ok; else bad "a1: expected ~2s, got ${CASE_ELAPSED}s"; fi
if pgrep -f "$CASE_SHIM" >/dev/null 2>&1; then
    bad "a1: shim process still alive after run_tlc returned"
else
    ok
fi
rm -rf "$CASE_TMP"

echo "--- (a2) budget 2s, shim ignores TERM: expect timeout verdict in ~32s, nothing survives"
run_tlc_case "$shim_ignores_term" 2
echo "    measured: exit=$CASE_CODE elapsed=${CASE_ELAPSED}s"
if [ "$CASE_CODE" -eq 124 ]; then ok; else bad "a2: expected mapped exit 124 (raw 137), got $CASE_CODE"; fi
if [ "$CASE_ELAPSED" -ge 25 ] && [ "$CASE_ELAPSED" -le 45 ]; then ok; else bad "a2: expected ~32s (2s budget + 30s kill-after), got ${CASE_ELAPSED}s"; fi
if pgrep -f "$CASE_SHIM" >/dev/null 2>&1; then
    bad "a2: shim process still alive after run_tlc returned (kill-after failed to reap it)"
else
    ok
fi
rm -rf "$CASE_TMP"

rm -f "$shim_sleeps_60" "$shim_ignores_term"

# --- (b) refusal without GNU timeout on PATH --------------------------------
# Build a minimal PATH: a bindir holding only bash plus symlinks to the
# coreutils check-tla.sh actually calls before it would reach a lane (git,
# basename, dirname, date, mktemp, grep, sed, awk, cat, tr, sha256sum, mkdir,
# rm, cut, wc), and a `java` shim that only proves it was never invoked. No
# timeout, no gtimeout.
echo "--- (b) no GNU timeout on PATH: expect exit 2, refusal, java never launched"
bdir="$(mktemp -d)"
marker="$bdir/java-was-invoked"
for tool in bash git basename dirname date mktemp grep sed awk cat tr sha256sum mkdir rm cut wc head tail printf env sh; do
    real="$(command -v "$tool" 2>/dev/null)" || continue
    ln -sf "$real" "$bdir/$tool"
done
javashim="$bdir/java"
cat > "$javashim" <<EOF
#!/usr/bin/env bash
case "\$1" in
    -version) echo 'openjdk version "21.0.4" 2024-07-16' >&2; exit 0 ;;
esac
touch "$marker"
exit 0
EOF
chmod +x "$javashim"

out=""
out="$(env -i PATH="$bdir" HOME="$HOME" RAVEL_TLA_JAVA="$javashim" "$SCRIPT" smoke 2>&1)"
code=$?

if [ "$code" -eq 2 ]; then ok; else bad "b: expected exit 2, got $code"; fi
if printf '%s' "$out" | grep -qF 'GNU timeout(1) not found'; then
    ok
else
    bad "b: refusal line not printed; got: $out"
fi
if [ -e "$marker" ]; then
    bad "b: java shim was invoked despite the missing-timeout refusal"
else
    ok
fi
rm -rf "$bdir"

# --- (c) normal operation is unchanged (see report; needs the real TLC jar
# and a JDK, so it is not run unpiped here) ----------------------------------
echo "(c) is proved manually and reported, not replayed here: it needs the" \
     "real TLC jar, network on first fetch, and a JDK, none of which this" \
     "unit test provisions."

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
