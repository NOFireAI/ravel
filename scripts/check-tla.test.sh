#!/usr/bin/env bash
# Proofs for issue #1192: check-tla.sh must enforce its TLC wall-clock
# budget via GNU timeout(1), never report a timed-out run as a pass, refuse
# outright when no GNU timeout is available or when one accepts the banner
# check but rejects --kill-after, and never require Java for the
# traceability lane. Run:
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
LIB_SRC="$(mktemp)"
source_lib() {
    sed '$d' "$SCRIPT" > "$LIB_SRC"
    # shellcheck disable=SC1090
    source "$LIB_SRC"
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
    resolve_tla_resources

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
# ':' and '>' are shell builtins/syntax, not external commands: the
# restricted PATH built above deliberately carries no 'touch', so an
# external touch here would fail silently and the marker would never
# land regardless of whether java was actually invoked.
: > "$marker"
case "\$1" in
    -version) echo 'openjdk version "21.0.4" 2024-07-16' >&2; exit 0 ;;
esac
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

# --- (d) refusal when timeout's banner lies about --kill-after -------------
# A candidate that prints the GNU coreutils --version banner but rejects
# --kill-after must be refused at startup, before java ever runs: accepting
# it on the banner alone would only fail once a lane is already mid-run.
echo "--- (d) timeout shim: real banner, --kill-after rejected: expect exit 2, refusal, java never launched"
ddir="$(mktemp -d)"
dmarker="$ddir/java-was-invoked"
for tool in bash git basename dirname date mktemp grep sed awk cat tr sha256sum mkdir rm cut wc head tail printf env sh; do
    real="$(command -v "$tool" 2>/dev/null)" || continue
    ln -sf "$real" "$ddir/$tool"
done
timeoutshim="$ddir/timeout"
cat > "$timeoutshim" <<'EOF'
#!/usr/bin/env bash
case "$1" in
    --version) echo 'timeout (GNU coreutils) 9.4'; exit 0 ;;
esac
for a in "$@"; do
    case "$a" in
        --kill-after*) exit 125 ;;
    esac
done
exit 0
EOF
chmod +x "$timeoutshim"
javashim_d="$ddir/java"
cat > "$javashim_d" <<EOF
#!/usr/bin/env bash
: > "$dmarker"
case "\$1" in
    -version) echo 'openjdk version "21.0.4" 2024-07-16' >&2; exit 0 ;;
esac
exit 0
EOF
chmod +x "$javashim_d"

out=""
out="$(env -i PATH="$ddir" HOME="$HOME" RAVEL_TLA_JAVA="$javashim_d" "$SCRIPT" smoke 2>&1)"
code=$?

if [ "$code" -eq 2 ]; then ok; else bad "d: expected exit 2, got $code"; fi
if printf '%s' "$out" | grep -qF 'GNU timeout(1) not found'; then
    ok
else
    bad "d: refusal line not printed; got: $out"
fi
if [ -e "$dmarker" ]; then
    bad "d: java shim was invoked despite the --kill-after refusal"
else
    ok
fi
rm -rf "$ddir"

# --- (e) traceability needs no Java -----------------------------------------
# main used to call resolve_java before dispatching on the subcommand, so
# traceability (pure filesystem, no TLC) refused with "no Java found" on a
# host with no JDK. Restrict PATH to what traceability actually needs (git,
# basename, dirname, date, mktemp, grep, sed, awk, cat, tr, mkdir, rm, cut,
# wc, head, tail, printf, env, sh) and give it no java and no shim.
echo "--- (e) no java, no shim on PATH: traceability must exit 0"
edir="$(mktemp -d)"
for tool in bash git basename dirname date mktemp grep sed awk cat tr mkdir rm cut wc head tail printf env sh; do
    real="$(command -v "$tool" 2>/dev/null)" || continue
    ln -sf "$real" "$edir/$tool"
done

out=""
out="$(env -i PATH="$edir" HOME="$HOME" "$SCRIPT" traceability 2>&1)"
code=$?

if [ "$code" -eq 0 ]; then ok; else bad "e: expected exit 0, got $code; output: $out"; fi
rm -rf "$edir"

# --- (f) traceability: two Rust references in one row (issue #1243) --------
# A property whose transition spans two symbols in two crates records both
# in the source-ref column, whitespace separated. Each reference must
# resolve independently against its own file: a passing two-ref row, and a
# failing one where only the second reference's symbol is missing (the bug
# this closes made a second file's symbol get grepped against the first
# file instead of its own).
echo "--- (f) traceability: multi-ref row resolves, and a bad second ref fails"
frepo="$(mktemp -d)"
mkdir -p "$frepo/crates" "$frepo/formal/tla/fakearea"
cat > "$frepo/crates/a.rs" <<'EOF'
struct Foo;
impl Foo {
    fn bar() {}
}
EOF
cat > "$frepo/crates/b.rs" <<'EOF'
fn Baz() {}
EOF
cat > "$frepo/formal/tla/fakearea/traceability.md" <<'EOF'
# Fake area traceability

| TLA+ action or property | meaning | Rust path and symbol | existing test | new test needed |
|---|---|---|---|---|
| TwoRefProp | a transition spanning two crates | `crates/a.rs::Foo::bar` `crates/b.rs::Baz` | none | none |
EOF

orig_repo_root="$REPO_ROOT"
orig_formal_dir="$FORMAL_DIR"
REPO_ROOT="$frepo"
FORMAL_DIR="$frepo/formal/tla"

fout=""
fout="$(check_traceability fakearea 2>&1)"
fcode=$?
if [ "$fcode" -eq 0 ]; then ok; else bad "f1: expected exit 0 on a valid two-ref row, got $fcode; output: $fout"; fi
if printf '%s' "$fout" | grep -qF 'PASS (1 rows resolve)'; then
    ok
else
    bad "f1: expected the row to be counted; output: $fout"
fi

# Break only the second reference's symbol: the row must fail, and the
# reported line must name the second file, never the first.
cat > "$frepo/formal/tla/fakearea/traceability.md" <<'EOF'
# Fake area traceability

| TLA+ action or property | meaning | Rust path and symbol | existing test | new test needed |
|---|---|---|---|---|
| TwoRefProp | a transition spanning two crates | `crates/a.rs::Foo::bar` `crates/b.rs::Qux` | none | none |
EOF
fout="$(check_traceability fakearea 2>&1)"
fcode=$?
if [ "$fcode" -ne 0 ]; then ok; else bad "f2: expected nonzero exit on a bad second ref, got 0; output: $fout"; fi
failing_line="$(printf '%s\n' "$fout" | grep -F "symbol 'Qux' not found in 'crates/b.rs'" | head -1)"
if [ -n "$failing_line" ]; then
    ok
    echo "    failure line: $failing_line"
else
    bad "f2: expected a \"symbol 'Qux' not found in 'crates/b.rs'\" line; output: $fout"
fi

REPO_ROOT="$orig_repo_root"
FORMAL_DIR="$orig_formal_dir"
rm -rf "$frepo"

# --- (c) normal operation is unchanged (see report; needs the real TLC jar
# and a JDK, so it is not run unpiped here) ----------------------------------
echo "(c) is proved manually and reported, not replayed here: it needs the" \
     "real TLC jar, network on first fetch, and a JDK, none of which this" \
     "unit test provisions."

# --- (g)-(j) issue #1421: RAVEL_TLA_WORKERS / RAVEL_TLA_XMX -----------------
# `-workers auto` claims every core on the host and TLC's heap was left
# uncapped; a shared or loaded box needs both fixed. A java shim that never
# launches TLC, only records its own argv, proves what run_tlc actually
# constructs without needing the real jar.
run_tlc_args_case() {
    # run_tlc_args_case -> sets CASE_ARGS_FILE (one argv token per line) and
    # CASE_TMP, via a java shim that records "$@" and exits 0.
    local tmp area_dir shim logfile
    tmp="$(mktemp -d)"
    area_dir="$tmp/area"
    mkdir -p "$area_dir"
    : > "$area_dir/smoke.cfg"
    shim="$tmp/java"
    cat > "$shim" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$@" > "$tmp/args"
exit 0
EOF
    chmod +x "$shim"

    FORMAL_DIR="$tmp"
    CACHE_DIR="$tmp/cache"
    mkdir -p "$CACHE_DIR"
    JAVA="$shim"
    JAR="/dev/null"
    resolve_timeout

    logfile="$tmp/tlc.log"
    run_tlc area module "$area_dir/smoke.cfg" 5 "$logfile" >/dev/null 2>&1
    CASE_ARGS_FILE="$tmp/args"
    CASE_TMP="$tmp"
}

# args_has_flag_value <file> <flag> <value> -> true if <flag> is followed by
# <value> on the next line (a space-separated pair, e.g. "-workers" "2").
args_has_flag_value() {
    awk -v f="$2" -v v="$3" '
        $0 == f { getline nxt; if (nxt == v) { found = 1 } }
        END { exit !found }
    ' "$1"
}

echo "--- (g) default resources: -workers 2 -Xmx2g"
unset RAVEL_TLA_WORKERS RAVEL_TLA_XMX
# Capture the exit code in a subshell: resolve_tla_resources calls `exit` on a
# bad value, so an in-process call can only ever leave $? at 0 (a real failure
# would kill this test), which makes the assertion vacuous. bash -c isolates
# the exit so a regression that made the defaults invalid would be caught.
gout="$(bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"; gcode=$?
if [ "$gcode" -eq 0 ]; then ok; else bad "g: resolve_tla_resources exited $gcode on defaults; out: $gout"; fi
resolve_tla_resources  # sets TLA_WORKERS/TLA_XMX in this shell for run_tlc_args_case
run_tlc_args_case
if grep -qxF -- '-Xmx2g' "$CASE_ARGS_FILE" 2>/dev/null; then
    ok
else
    bad "g: expected -Xmx2g in argv; got: $(cat "$CASE_ARGS_FILE" 2>/dev/null)"
fi
if args_has_flag_value "$CASE_ARGS_FILE" -workers 2; then
    ok
else
    bad "g: expected -workers 2 in argv; got: $(cat "$CASE_ARGS_FILE" 2>/dev/null)"
fi
rm -rf "$CASE_TMP"

echo "--- (h) override RAVEL_TLA_WORKERS=4 RAVEL_TLA_XMX=4g"
hout="$(RAVEL_TLA_WORKERS=4 RAVEL_TLA_XMX=4g bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"; hcode=$?
if [ "$hcode" -eq 0 ]; then ok; else bad "h: resolve_tla_resources exited $hcode on a valid override; out: $hout"; fi
RAVEL_TLA_WORKERS=4 RAVEL_TLA_XMX=4g resolve_tla_resources
run_tlc_args_case
if grep -qxF -- '-Xmx4g' "$CASE_ARGS_FILE" 2>/dev/null; then
    ok
else
    bad "h: expected -Xmx4g in argv; got: $(cat "$CASE_ARGS_FILE" 2>/dev/null)"
fi
if args_has_flag_value "$CASE_ARGS_FILE" -workers 4; then
    ok
else
    bad "h: expected -workers 4 in argv; got: $(cat "$CASE_ARGS_FILE" 2>/dev/null)"
fi
rm -rf "$CASE_TMP"
unset RAVEL_TLA_WORKERS RAVEL_TLA_XMX

echo "--- (i) RAVEL_TLA_WORKERS=auto is accepted (CI opt-in) and reaches TLC"
iout=""
iout="$(RAVEL_TLA_WORKERS=auto bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"
icode=$?
echo "    measured: exit=$icode"
if [ "$icode" -eq 0 ]; then ok; else bad "i: expected exit 0 (auto accepted), got $icode; out: $iout"; fi
RAVEL_TLA_WORKERS=auto resolve_tla_resources
run_tlc_args_case
if args_has_flag_value "$CASE_ARGS_FILE" -workers auto; then
    ok
else
    bad "i: expected -workers auto in argv; got: $(cat "$CASE_ARGS_FILE" 2>/dev/null)"
fi
rm -rf "$CASE_TMP"
unset RAVEL_TLA_WORKERS

echo "--- (j) RAVEL_TLA_XMX=lots is rejected"
jout=""
jout="$(RAVEL_TLA_XMX=lots bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"
jcode=$?
echo "    measured: exit=$jcode"
if [ "$jcode" -eq 2 ]; then ok; else bad "j: expected exit 2, got $jcode"; fi
if printf '%s' "$jout" | grep -qF 'RAVEL_TLA_XMX'; then
    ok
else
    bad "j: refusal message missing RAVEL_TLA_XMX; got: $jout"
fi

# --- (k)-(m) issue #1421 item 6: a zero heap and an out-of-range worker count
# must be rejected with a message, not passed through to a JVM that refuses to
# start (a FAIL row per config) or a leaked raw bash "[: integer expression
# expected" -----------------------------------------------------------------
echo "--- (k) RAVEL_TLA_XMX=0g is rejected (zero heap)"
kout="$(RAVEL_TLA_XMX=0g bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"; kcode=$?
echo "    measured: exit=$kcode"
if [ "$kcode" -eq 2 ]; then ok; else bad "k: expected exit 2, got $kcode; out: $kout"; fi
if printf '%s' "$kout" | grep -qF 'RAVEL_TLA_XMX'; then ok; else bad "k: refusal missing RAVEL_TLA_XMX; got: $kout"; fi

echo "--- (l) RAVEL_TLA_XMX=0m is rejected (zero heap)"
lout="$(RAVEL_TLA_XMX=0m bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"; lcode=$?
echo "    measured: exit=$lcode"
if [ "$lcode" -eq 2 ]; then ok; else bad "l: expected exit 2, got $lcode; out: $lout"; fi
if printf '%s' "$lout" | grep -qF 'RAVEL_TLA_XMX'; then ok; else bad "l: refusal missing RAVEL_TLA_XMX; got: $lout"; fi

echo "--- (m) RAVEL_TLA_WORKERS=999999999999999999999 is rejected without leaking a bash error"
mout="$(RAVEL_TLA_WORKERS=999999999999999999999 bash -c "source '$LIB_SRC'; resolve_tla_resources" 2>&1)"; mcode=$?
echo "    measured: exit=$mcode"
if [ "$mcode" -eq 2 ]; then ok; else bad "m: expected exit 2, got $mcode; out: $mout"; fi
if printf '%s' "$mout" | grep -qF 'RAVEL_TLA_WORKERS'; then ok; else bad "m: refusal missing RAVEL_TLA_WORKERS; got: $mout"; fi
if printf '%s' "$mout" | grep -qF 'integer expression expected'; then
    bad "m: leaked raw bash '[: integer expression expected' to stderr; out: $mout"
else
    ok
fi

# --- (n) traceability: zero resolved rows fails, naming the area (issue #1356)
# A traceability.md with only a header row and no data rows must fail, not
# pass: a table that lost every row (a bad edit, a rebase) is otherwise
# indistinguishable from one that legitimately has nothing left to check.
echo "--- (n) traceability: zero resolved rows fails, naming the area"
nrepo="$(mktemp -d)"
mkdir -p "$nrepo/crates" "$nrepo/formal/tla/emptyarea"
cat > "$nrepo/formal/tla/emptyarea/traceability.md" <<'EOF'
# Empty area traceability

| TLA+ action or property | meaning | Rust path and symbol | existing test | new test needed |
|---|---|---|---|---|
EOF

orig_repo_root2="$REPO_ROOT"
orig_formal_dir2="$FORMAL_DIR"
REPO_ROOT="$nrepo"
FORMAL_DIR="$nrepo/formal/tla"

nout="$(check_traceability emptyarea 2>&1)"
ncode=$?
if [ "$ncode" -ne 0 ]; then ok; else bad "n: expected nonzero exit on zero resolved rows, got 0; output: $nout"; fi
if printf '%s' "$nout" | grep -qF "emptyarea traceability: FAIL (zero rows resolved)"; then
    ok
else
    bad "n: expected 'emptyarea traceability: FAIL (zero rows resolved)'; output: $nout"
fi

REPO_ROOT="$orig_repo_root2"
FORMAL_DIR="$orig_formal_dir2"
rm -rf "$nrepo"

# --- (o)-(p) issue #1356/#1357 finding 1: live gated by bands.tsv ----------
# check_model must not run an area's live.cfg unless bands.tsv carries a row
# for it (a measured band is the opt-in): unbanded reports SKIP and never
# launches TLC, banded runs it. A java shim that writes a canned TLC-shaped
# log and a marker file (instead of the real jar) proves both which path ran
# and whether TLC was actually launched.
build_fake_live_area() {
    # build_fake_live_area <with-band: yes|no> -> sets FAKE_TMP.
    local with_band="$1"
    local tmp area_dir shim
    tmp="$(mktemp -d)"
    area_dir="$tmp/formal/tla/fakelive"
    mkdir -p "$area_dir"
    cat > "$area_dir/MCFake.tla" <<'EOF'
---- MODULE MCFake ----
====
EOF
    cat > "$area_dir/smoke.cfg" <<'EOF'
SPECIFICATION Spec
EOF
    cat > "$area_dir/live.cfg" <<'EOF'
SPECIFICATION Spec
PROPERTY Prop
EOF
    if [ "$with_band" = yes ]; then
        printf 'cfg\tmin_distinct\tmax_distinct\tmin_depth\tmax_depth\nlive.cfg\t50\t50\t5\t5\n' \
            > "$area_dir/bands.tsv"
    fi
    shim="$tmp/java"
    cat > "$shim" <<EOF
#!/usr/bin/env bash
touch "$tmp/java-ran"
cat <<'LOG'
100 states generated, 50 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 5.
Model checking completed. No error has been found.
LOG
exit 0
EOF
    chmod +x "$shim"
    FAKE_TMP="$tmp"
}

orig_formal_dir3="$FORMAL_DIR"
orig_cache_dir3="$CACHE_DIR"
orig_log_dir3="$LOG_DIR"
orig_last_run3="$LAST_RUN"
orig_java3="${JAVA:-}"
orig_jar3="${JAR:-}"

echo "--- (o) live: no bands.tsv row -> SKIP, TLC never invoked, exit 0"
build_fake_live_area no
FORMAL_DIR="$FAKE_TMP/formal/tla"
CACHE_DIR="$FAKE_TMP/cache"
LOG_DIR="$CACHE_DIR/logs"
LAST_RUN="$CACHE_DIR/last-run.tsv"
JAVA="$FAKE_TMP/java"
JAR="/dev/null"
resolve_timeout
resolve_tla_resources
oout="$(check_model fakelive live 2>&1)"; ocode=$?
echo "    measured: exit=$ocode"
if [ "$ocode" -eq 0 ]; then ok; else bad "o: expected exit 0 on an unbanded live skip, got $ocode; output: $oout"; fi
if printf '%s' "$oout" | grep -qF "fakelive live: SKIP (unbanded live.cfg; add a bands.tsv row to enrol)"; then
    ok
else
    bad "o: expected the SKIP message; output: $oout"
fi
if [ -e "$FAKE_TMP/java-ran" ]; then
    bad "o: java shim ran despite the cfg being unbanded"
else
    ok
fi
rm -rf "$FAKE_TMP"

echo "--- (p) live: bands.tsv carries a matching row -> runs and PASSes"
build_fake_live_area yes
FORMAL_DIR="$FAKE_TMP/formal/tla"
CACHE_DIR="$FAKE_TMP/cache"
LOG_DIR="$CACHE_DIR/logs"
LAST_RUN="$CACHE_DIR/last-run.tsv"
JAVA="$FAKE_TMP/java"
JAR="/dev/null"
resolve_timeout
resolve_tla_resources
pout="$(check_model fakelive live 2>&1)"; pcode=$?
echo "    measured: exit=$pcode"
if [ "$pcode" -eq 0 ]; then ok; else bad "p: expected exit 0 on a banded live pass, got $pcode; output: $pout"; fi
if [ -e "$FAKE_TMP/java-ran" ]; then
    ok
else
    bad "p: expected the java shim to be launched once the cfg is banded; output: $pout"
fi
if printf '%s' "$pout" | grep -qF "fakelive/MCFake live: PASS"; then
    ok
else
    bad "p: expected a PASS line for fakelive/MCFake; output: $pout"
fi
rm -rf "$FAKE_TMP"

FORMAL_DIR="$orig_formal_dir3"
CACHE_DIR="$orig_cache_dir3"
LOG_DIR="$orig_log_dir3"
LAST_RUN="$orig_last_run3"
JAVA="$orig_java3"
JAR="$orig_jar3"

rm -f "$LIB_SRC"
printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
