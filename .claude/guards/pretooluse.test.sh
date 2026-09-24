#!/usr/bin/env bash
# Cases the PreToolUse guard must get right. Run: bash .claude/guards/pretooluse.test.sh
set -uo pipefail

GUARD="$(cd "$(dirname "$0")" && pwd)/pretooluse.mjs"
pass=0
fail=0

# want: allow | deny
check() {
  local want="$1" label="$2" payload="$3" out decision
  out=$(printf '%s' "$payload" | node "$GUARD" 2>&1) || {
    printf 'FAIL  %-52s guard exited non-zero: %s\n' "$label" "$out"
    fail=$((fail + 1))
    return
  }
  if printf '%s' "$out" | grep -q '"permissionDecision":"deny"'; then
    decision=deny
  else
    decision=allow
  fi
  if [ "$decision" = "$want" ]; then
    pass=$((pass + 1))
  else
    printf 'FAIL  %-52s want %s, got %s\n' "$label" "$want" "$decision"
    fail=$((fail + 1))
  fi
}

bash_payload() {
  node -e 'process.stdout.write(JSON.stringify({tool_name:"Bash",tool_input:{command:process.argv[1]}}))' "$1"
}

# --- gate masking -------------------------------------------------------
check deny  "cargo test piped to tail"        "$(bash_payload 'cargo test -p ravel-sql | tail -20')"
check deny  "gates.sh piped to grep"          "$(bash_payload 'scripts/gates.sh | grep -E "All gates"')"
check deny  "clippy with cd prefix, to head"  "$(bash_payload 'cd /repo && cargo clippy --workspace | head -40')"
check deny  "nextest && echo MARKER"          "$(bash_payload 'cargo nextest run && echo DONE')"
check allow "cargo test alone"                "$(bash_payload 'cargo test -p ravel-sql')"
check allow "cargo test redirected to a file" "$(bash_payload 'cargo test -p ravel-sql > /tmp/out.txt 2>&1')"
check allow "grep for the words cargo test"   "$(bash_payload 'grep -rn "cargo test" docs/ | head -5')"
check allow "cargo metadata into jq"          "$(bash_payload 'cargo metadata --format-version 1 | jq -r .packages')"
# The rule reads as being about gates and is not: a guard's exit code is read
# the same way. Both of these were run by real sessions on 2026-09-09 and both
# reported a false pass, one on a stale branch and one on a guard that was not
# in the checkout at all.
check deny  "guard piped to head"              "$(bash_payload 'scripts/guards/assert-fresh-merge-base.sh 1556 | head -3')"
check deny  "guard captured through a pipe"    "$(bash_payload 'out=$(scripts/guards/assert-fresh-merge-base.sh 1556 2>&1 | tail -1)')"
check deny  "guard piped to grep"              "$(bash_payload 'scripts/guards/check-disk-headroom.sh . 20 | grep LOW')"
check allow "guard with no pipe"               "$(bash_payload 'scripts/guards/assert-fresh-merge-base.sh 1556')"
check allow "guard output read from a file"    "$(bash_payload 'scripts/guards/assert-fresh-merge-base.sh 1556 > /tmp/g.txt 2>&1')"
# An allow case only has teeth when something could deny it. This one carried
# no pipe and no `&& echo`, so no rule could ever have fired on it and it
# passed against every mutation of the prefix list. What the harmless-prefix
# list actually has to do is let the gate BEHIND the prefix still be seen.
check deny  "an env prefix does not launder a gate" \
  "$(bash_payload 'CARGO_INCREMENTAL=0 cargo test -p ravel-sql | tail -20')"
check allow "an env prefix before an unpiped gate" \
  "$(bash_payload 'CARGO_INCREMENTAL=0 cargo test -p ravel-sql')"

# Quoting the substitution, or spelling it with backticks, runs the same gate
# and reads the same pipe status. Each of these was allowed while the bare
# `out=$(...)` form above was denied.
check deny  "guard in a quoted substitution"   "$(bash_payload 'out="$(scripts/guards/assert-fresh-merge-base.sh 1556 | tail -1)"')"
check deny  "guard in a backtick substitution" "$(bash_payload 'out=`scripts/guards/assert-fresh-merge-base.sh 1556 | tail -1`')"
check deny  "gate in a bare substitution"      "$(bash_payload 'echo "$(cargo test -p ravel-sql | tail -5)"')"
check deny  "gate substituted inside a string" "$(bash_payload 'git commit -m "ran $(scripts/gates.sh | grep -c passed)"')"
check deny  "gate in a nested substitution"    "$(bash_payload 'x=$(echo "$(cargo clippy --workspace | head -3)")')"
check deny  "guard piped inside an if"         "$(bash_payload 'if scripts/guards/check-disk-headroom.sh . 20 | grep -q LOW; then echo low; fi')"
# No substitution happens inside single quotes, so no gate runs and there is
# nothing to mask.
check allow "substitution syntax, single-quoted" "$(bash_payload "echo 'ran \$(cargo test | tail -1)'")"
check allow "the text of a gate pipe, quoted"  "$(bash_payload "echo 'cargo test | tail -5'")"
check allow "an unpiped guard in a quoted sub" "$(bash_payload 'out="$(scripts/guards/assert-fresh-merge-base.sh 1556)"')"
check allow "jq inside a quoted substitution"  "$(bash_payload 'n="$(cargo metadata --format-version 1 | jq -r .packages)"')"

# Both of these were run for real by this session on 2026-09-10, minutes after
# the substitution hole above was closed, and both reported an empty exit code
# through a `tail`. `timeout` was not a recognised prefix, and a guard's own
# test suite did not match the guard pattern because `disk-watchdog.test.sh`
# carries a second dot.
check deny  "a gate behind timeout"            "$(bash_payload 'timeout 60 scripts/gates.sh | tail -5')"
check deny  "a guard suite behind timeout+bash" "$(bash_payload 'timeout 300 bash scripts/guards/disk-watchdog.test.sh | tail -25')"
check deny  "a guard test suite piped"         "$(bash_payload 'bash scripts/guards/disk-watchdog.test.sh | tail -25')"
check deny  "the hook's own suite piped"       "$(bash_payload 'bash .claude/guards/pretooluse.test.sh | tail -5')"
check allow "a guard suite redirected"         "$(bash_payload 'bash scripts/guards/disk-watchdog.test.sh > /tmp/w.txt 2>&1')"
check allow "timeout on a non-gate"            "$(bash_payload 'timeout 5 curl -s https://example.com | head -3')"

check allow "git log into head"               "$(bash_payload 'git log --oneline | head -5')"
check allow "grepping a saved gate log"       "$(bash_payload 'grep -c FAILED /tmp/gate.log')"

# --- heredoc bodies are data, not shell ----------------------------------
#
# Every payload above is a single line, which is why this class was invisible.
# Statements split on newlines, so a document QUOTING a piped gate was refused
# line by line: the commit message describing these very rules could not be
# written by the tool that writes commit messages. The exemption has to drop
# the BODY rather than the whole command, since `command.includes("<<")` also
# excuses a real gate that happens to share a call with a heredoc.
gate_pipe='cargo test -p ravel-sql | tail -5'
check allow "a heredoc body quoting a piped gate" \
  "$(bash_payload "cat > /tmp/c.md <<'EOF'
${gate_pipe} was allowed because timeout was not a prefix.
EOF")"
check allow "a heredoc body quoting a piped suite" \
  "$(bash_payload "cat > /tmp/c.md <<'EOF'
bash scripts/guards/disk-watchdog.test.sh | tail -25 said nothing.
EOF")"
check allow "an unquoted heredoc delimiter" \
  "$(bash_payload "cat > /tmp/c.md <<EOF
${gate_pipe}
EOF")"
check deny  "a real piped gate on a later line" \
  "$(bash_payload "echo hi
${gate_pipe}")"
check deny  "a heredoc beside a real piped gate" \
  "$(bash_payload "cat > /tmp/c.md <<'EOF'
harmless text
EOF
${gate_pipe}")"
check deny  "a reserved name beside a heredoc" \
  "$(bash_payload "cat > /tmp/c.md <<'EOF'
harmless text
EOF
status=0")"

# --- a substitution VALUE must not hide the gate after it ----------------
#
# Found by review, and not by the 24-case battery written to look for it:
# every case there put the gate INSIDE the substitution. This shape is an
# assignment whose value is a NON-gate substitution, with the gate after the
# closing paren. A `NAME=$(` prefix alternative consumed `FOO=$(` and left
# `date) cargo test`, which is not a gate, so the guard ALLOWED a masked gate
# that `main` denied. The second case has a space inside the substitution and
# was a hole on `main` too.
check deny  "substitution value, then a gate"  "$(bash_payload 'FOO=$(date) cargo test -p ravel-sql | tail -1')"
check deny  "substitution value, then a guard" "$(bash_payload 'FOO=$(date) scripts/gates.sh | tail -1')"
check deny  "substitution value, then && echo" "$(bash_payload 'FOO=$(date) cargo test -p ravel-sql && echo DONE')"
check deny  "a space inside the substitution"  "$(bash_payload 'TS=$(date +%s) cargo test -p ravel-sql | tail -1')"
check deny  "backtick value, then a gate"      "$(bash_payload 'FOO=`date` cargo test -p ravel-sql | tail -1')"

# --- the heredoc strip must fail closed ----------------------------------
#
# Dropping a body to end-of-input when no terminator exists deletes every
# remaining line from every rule. A herestring and a bare `<<` inside a
# quoted string both matched as openers, so the rule's own motivating case,
# quoting shell in a commit message, disabled the guard for the rest of the
# command.
check deny  "herestring is not a heredoc"      "$(bash_payload "cat <<<WORD
${gate_pipe}")"
check deny  "quoted herestring is not one"     "$(bash_payload "python3 - <<<'print(1)'
${gate_pipe}")"
check deny  "<< inside a commit message"       "$(bash_payload "git commit -m \"use << HEAD trick\"
${gate_pipe}")"
check deny  "<< inside single quotes"          "$(bash_payload "echo 'a << B'
${gate_pipe}")"
check deny  "unterminated heredoc keeps lines" "$(bash_payload "cat > /tmp/c.md <<EOF
${gate_pipe}")"
# A herestring whose word happens to match a later line. The terminator check
# alone does not save this one, because a terminator IS found: only refusing
# to read `<<<` as an opener does. Without that, the gate on line two is
# stripped as a heredoc body and the command is allowed.
check deny  "herestring whose word recurs"     "$(bash_payload "cat <<<EOF
${gate_pipe}
EOF")"
# Longer runs of `<` are not valid shell, but the guard must answer the same
# way for all of them. Skipping a fixed two characters made `<<<<` fail closed
# and `<<<<<` fail open; consuming the whole run makes every length behave
# like the herestring above. Both lengths, since one alone cannot see the
# alternation.
check deny  "four angles is not an opener"     "$(bash_payload "cat <<<<EOF
${gate_pipe}
EOF")"
check deny  "five angles is not an opener"     "$(bash_payload "cat <<<<<EOF
${gate_pipe}
EOF")"

# `<<` inside a QUOTED STRING is ordinary text, and a later line that happens
# to equal the word parsed out of it is not a terminator. Fail-closed does not
# save these, because a terminator is found; only tracking quote state does.
# Every one was refused on main and allowed here until the opener scan stopped
# being a regex. Note each needs THREE lines: the two-line versions above take
# the fail-closed path instead and passed throughout, which is why hand-written
# cases missed the whole class.
check deny  "single-quoted << , tag recurs"    "$(bash_payload "git commit -m 'use << EOF here'
${gate_pipe}
EOF")"
check deny  "single-quoted << , short tag"     "$(bash_payload "echo 'a << B'
${gate_pipe}
B")"
check deny  "double-quoted << , tag recurs"    "$(bash_payload "echo \"shift << N\"
${gate_pipe}
N")"
check deny  "quoted << , tab-indented body"    "$(bash_payload "git commit -m 'use << EOF here'
	${gate_pipe}
EOF")"
check deny  "quoted << , reserved name after"  "$(bash_payload "echo 'a << B'
status=0
B")"
# The delimiter's own quotes must not leak into the walk's quote state: if
# `<<'EOF'` left the scanner inside a string, everything after it on the line
# would be misread.
check allow "quoted delimiter closes cleanly"  "$(bash_payload "cat > /tmp/c.md <<'EOF' && echo queued
${gate_pipe}
EOF")"
# Two heredocs, the first with a QUOTED delimiter and the gate in the second
# body. This is what pins consuming the delimiter's own closing quote: leave
# it unconsumed and the walk thinks it is inside a string for the rest of the
# line, never sees `<<B`, and judges B's body as live shell. The single-
# heredoc case above cannot detect that, because nothing follows the
# delimiter that the walk needs to read.
check allow "a second heredoc after a quoted one" "$(bash_payload "cat <<'A' <<B
first body
A
${gate_pipe}
B")"

# A terminator that IS found still ends the body, and the body's later lines
# stay data. With terminator matching broken to stop at the first body line,
# line two leaks out and is judged, so this flips to deny.
check allow "second body line stays data"      "$(bash_payload "cat > /tmp/c.md <<'EOF'
harmless first line
${gate_pipe}
EOF")"
# `<<-` strips leading TABS from the terminator. Nothing pinned that branch.
check allow "<<- with a tab-indented end"      "$(bash_payload "cat > /tmp/c.md <<-EOF
${gate_pipe}
	EOF")"

# --- zsh reserved names -------------------------------------------------
check deny  "bare status="                    "$(bash_payload 'status=0')"
check deny  "local status="                   "$(bash_payload 'run_it || local status=$?')"
check deny  "path= after semicolon"           "$(bash_payload 'echo hi; path=/tmp')"
check allow "rc= is fine"                     "$(bash_payload 'run_it || rc=$?')"
check allow "PATH= is not path="              "$(bash_payload 'export PATH=/usr/bin:$PATH')"
check allow "--status= flag"                  "$(bash_payload 'gh pr list --status=open')"
check allow "status inside a jq filter"       "$(bash_payload "jq -r 'select(.status==\"done\")' t.json")"
check allow "python heredoc assigning path"   "$(bash_payload 'python3 - <<PY
path="/tmp/x"
print(path)
PY')"

# --- ScheduleWakeup -----------------------------------------------------
wakeup() {
  node -e 'process.stdout.write(JSON.stringify({tool_name:"ScheduleWakeup",tool_input:JSON.parse(process.argv[1])}))' "$1"
}
check deny  "wakeup 300s"                     "$(wakeup '{"delaySeconds":300,"noop":true}')"
check deny  "wakeup 600s"                     "$(wakeup '{"delaySeconds":600,"noop":false}')"
check allow "wakeup 900s"                     "$(wakeup '{"delaySeconds":900,"noop":true}')"
check allow "wakeup 1800s"                    "$(wakeup '{"delaySeconds":1800,"noop":true}')"
check allow "wakeup stop"                     "$(wakeup '{"stop":true}')"

# --- Edit/Write worktree isolation --------------------------------------
write_to() {
  node -e 'process.stdout.write(JSON.stringify({tool_name:"Edit",tool_input:{file_path:process.argv[1]}}))' "$1"
}
tmproot=$(mktemp -d)
mkdir -p "$tmproot/primary/.git/worktrees/wt" "$tmproot/primary/src"
: > "$tmproot/primary/src/lib.rs"
mkdir -p "$tmproot/linked/src"
printf 'gitdir: %s/primary/.git/worktrees/wt\n' "$tmproot" > "$tmproot/linked/.git"
: > "$tmproot/linked/src/lib.rs"
mkdir -p "$tmproot/clone/.git" "$tmproot/clone/src"
: > "$tmproot/clone/src/lib.rs"
mkdir -p "$tmproot/scratch"
: > "$tmproot/scratch/notes.md"

check deny  "edit inside the primary checkout" "$(write_to "$tmproot/primary/src/lib.rs")"
check allow "edit inside a linked worktree"    "$(write_to "$tmproot/linked/src/lib.rs")"
check allow "edit in a clone with no worktrees" "$(write_to "$tmproot/clone/src/lib.rs")"
check allow "edit outside any repo"            "$(write_to "$tmproot/scratch/notes.md")"
# Subshell so the escape hatch cannot leak into later cases; the subshell's
# own counters are lost, so score it here.
if (
  export RAVEL_GUARD_ALLOW_PRIMARY=1
  printf '%s' "$(write_to "$tmproot/primary/src/lib.rs")" | node "$GUARD" |
    grep -q '"permissionDecision":"deny"'
); then
  printf 'FAIL  %-52s want allow, got deny\n' "escape hatch env set"
  fail=$((fail + 1))
else
  pass=$((pass + 1))
fi
rm -rf "$tmproot"

# --- cargo's own flags sit before the subcommand ------------------------
# `cargo --locked test | tail -1` ran the gate and read the pipe's status,
# and the guard allowed it: the pattern was anchored on `cargo` followed
# immediately by the subcommand, which none of cargo's global flags leave
# intact.
check deny  "cargo --locked test piped to tail" "$(bash_payload 'cargo --locked test | tail -1')"
check deny  "cargo --offline clippy to head"    "$(bash_payload 'cargo --offline clippy --workspace | head -3')"
check deny  "cargo +toolchain test piped"       "$(bash_payload 'cargo +nightly test -p ravel-sql | tail -5')"
check allow "cargo --locked test alone"         "$(bash_payload 'cargo --locked test -p ravel-sql')"

# --- destructive git ----------------------------------------------------
# These discard work nothing can recover. The pre-push hook covers the push
# at the git level, but it never sees a reset or a filter-branch, and it is
# only installed where someone ran the installer.
check deny  "reset --hard onto origin/main"     "$(bash_payload 'git reset --hard origin/main')"
check deny  "reset --soft onto origin/main"     "$(bash_payload 'git reset --soft origin/main')"
check deny  "reset --hard onto @{upstream}"     "$(bash_payload 'git reset --hard @{upstream}')"
check deny  "reset --hard onto FETCH_HEAD"      "$(bash_payload 'git reset --hard FETCH_HEAD')"
check deny  "reset with the flag written last"  "$(bash_payload 'git reset origin/main --hard')"
check deny  "filter-branch"                     "$(bash_payload 'git filter-branch --tree-filter "rm -f secret" HEAD')"
check deny  "push --force to main"              "$(bash_payload 'git push --force origin main')"
check deny  "push -f to main"                   "$(bash_payload 'git push -f origin main')"
check deny  "push --force-with-lease to main"   "$(bash_payload 'git push --force-with-lease origin main')"
check deny  "push +refs/heads/main"             "$(bash_payload 'git push origin +refs/heads/main')"
check deny  "push HEAD:main forced"             "$(bash_payload 'git push --force origin HEAD:main')"

# Ordinary work is not blocked.
check allow "reset --hard to a local sha"       "$(bash_payload 'git reset --hard HEAD~1')"
check allow "reset --soft HEAD~1"               "$(bash_payload 'git reset --soft HEAD~1')"
check allow "plain push to main"                "$(bash_payload 'git push origin main')"
check allow "force-push to a feature branch"    "$(bash_payload 'git push --force origin feat/x')"
check allow "the words in a grep pattern"       "$(bash_payload 'grep -rn "git reset --hard origin/main" docs/')"
check allow "the words in a commit message"     "$(bash_payload 'git commit -m "document git push --force origin main"')"

# The escape hatch has to be read from the command TEXT: shell state does not
# survive between tool calls, so an inline assignment is the only spelling
# that can work, and it is exactly what the harmless-prefix list strips.
check allow "ALLOW_DESTRUCTIVE=1 before a reset" \
  "$(bash_payload 'ALLOW_DESTRUCTIVE=1 git reset --hard origin/main')"
check allow "ALLOW_DESTRUCTIVE=1 before a force-push" \
  "$(bash_payload 'ALLOW_DESTRUCTIVE=1 git push --force origin main')"
check deny  "a different env prefix does not launder it" \
  "$(bash_payload 'CARGO_INCREMENTAL=0 git reset --hard origin/main')"

# Review findings on this PR, each verified against the hook before the fix.
# All three are regex-boundary holes of the same shape: a pattern that is
# right about plain text and wrong about one ordinary variation of it.
#
# A quote between the space and the ref. `git reset --hard 'origin/main'`
# discards exactly what the unquoted spelling does.
check deny  "reset --hard 'origin/main' single-quoted" "$(bash_payload "git reset --hard 'origin/main'")"
check deny  "reset --hard \"origin/main\" double-quoted" "$(bash_payload 'git reset --hard "origin/main"')"
check deny  "reset --soft, quoted remote ref"          "$(bash_payload 'git reset --soft "origin/main"')"
# The abbreviated plus-refspec. Requiring `+refs/` matched only the long form
# and made PUSH_TARGETS_MAIN's own `+main`/`HEAD:main` alternatives dead.
check deny  "push origin +main"                        "$(bash_payload 'git push origin +main')"
check deny  "push origin +HEAD:main"                   "$(bash_payload 'git push origin +HEAD:main')"
check deny  "push origin +main:main"                   "$(bash_payload 'git push origin +main:main')"
# A branch whose name merely STARTS with main is not main. `main\b` refused
# these, which is fail-closed but wrong, and the override would have been
# used to work around the guard rather than to accept a loss.
check allow "force-push to main-experiment"            "$(bash_payload 'git push --force-with-lease origin main-experiment')"
check allow "force-push to main/foo"                   "$(bash_payload 'git push --force origin main/foo')"
check allow "force-push to maintenance"                "$(bash_payload 'git push --force origin maintenance')"
check allow "force-push to mainline"                   "$(bash_payload 'git push --force origin mainline')"

# --- bare 40-hex-char SHA literal ----------------------------------------
# A full SHA typed into a command is almost always completed from a shorter
# prefix read off adjacent output, and a wrong digit has been caught by the
# receiving system six times in this repository's sessions, never by the
# person. SHA40 below is exactly 40 hex characters; SHA39/SHA41 are one
# character short and one character over.
SHA40='abc123def456abc123def456abc123def456abcd'
SHA39='abc123def456abc123def456abc123def456abc'
SHA41='abc123def456abc123def456abc123def456abcde'

check deny  "bare 40-hex literal on --match-head-commit" \
  "$(bash_payload "gh pr merge 123 --match-head-commit ${SHA40}")"
check deny  "bare 40-hex literal, quoted" \
  "$(bash_payload "gh pr merge 123 --match-head-commit \"${SHA40}\"")"
check deny  "bare 40-hex literal inside a command substitution" \
  "$(bash_payload "x=\$(git log -1 --format=%H ${SHA40})")"
check allow "escape hatch permits a bare sha literal" \
  "$(bash_payload "ALLOW_LITERAL_SHA=1 gh pr merge 123 --match-head-commit ${SHA40}")"
check deny  "a different env prefix does not launder a sha literal" \
  "$(bash_payload "CARGO_INCREMENTAL=0 gh pr merge 123 --match-head-commit ${SHA40}")"
check allow "resolving the sha via substitution instead of a literal" \
  "$(bash_payload 'sha=$(git rev-parse origin/main) && gh pr merge 123 --match-head-commit "$sha"')"

# A 39- or 41-char hex run is not a SHA-sized literal, and neither is a
# 40-char window sitting inside a longer unbroken hex run: the boundary on
# either side is still hex, so no window can satisfy it.
check allow "39-char hex run is not a sha literal" \
  "$(bash_payload "echo ${SHA39}")"
check allow "41-char hex run is not a sha literal" \
  "$(bash_payload "echo ${SHA41}")"
check allow "40-char window inside a longer hex run (extended right)" \
  "$(bash_payload "echo ${SHA40}e")"
check allow "40-char window inside a longer hex run (extended left)" \
  "$(bash_payload "echo 1${SHA40}")"

# A heredoc body is data, not shell, same as the gate-masking rule above: a
# fixture file legitimately contains a bare SHA as text, not as a command
# argument, and needs no escape hatch.
check allow "a bare sha inside a heredoc body" \
  "$(bash_payload "cat > /tmp/fixture.txt <<'EOF'
${SHA40}
EOF")"

# Known false positives, decided deliberately rather than silently allowed:
# reading history with a full SHA is low-stakes (a typo just errors as an
# unknown revision), but it is still a hand-typed literal of the exact shape
# this rule exists to catch, and the fix (resolve it, or use the escape
# hatch) costs nothing extra. No exemption for read-only git subcommands.
check deny  "git show naming a full sha" \
  "$(bash_payload "git show ${SHA40}")"
check deny  "git log naming a full sha" \
  "$(bash_payload "git log -1 ${SHA40}")"
# A non-SHA 40-hex literal (an HMAC, a test vector) is denied the same way:
# the guard cannot tell a SHA from any other 40-hex string, and it should
# not try to, since a hand-typed hex-shaped literal is the same failure
# shape either way. The escape hatch covers this case too.
check deny  "a non-sha 40-hex literal (hmac-shaped)" \
  "$(bash_payload "curl -H \"Authorization: Bearer ${SHA40}\" https://example.com")"
check allow "escape hatch permits a non-sha 40-hex literal" \
  "$(bash_payload "ALLOW_LITERAL_SHA=1 curl -H \"Authorization: Bearer ${SHA40}\" https://example.com")"

# When the literal IS the check, it must be kept, not re-resolved: a
# --match-head-commit value re-resolved at merge time matches whatever the
# head is by then, so a push landing after review would merge unreviewed.
# scripts/pr-review-status.sh prints its merge line with the escape hatch
# already on it. Take every line it can print, exactly as it prints it, and
# feed each through the guard: if the script ever loses the prefix, or the
# guard stops honouring it after an `&&`, these fail rather than drifting.
PRS="$(dirname "$0")/../../scripts/pr-review-status.sh"
merge_lines=0
while IFS= read -r line; do
  cmd="${line#*-> }"
  cmd="${cmd%\"}"
  cmd="${cmd//\$\{pr\}/123}"
  cmd="${cmd//\$\{base_ref\}/main}"
  cmd="${cmd//\$\{head_sha\}/${SHA40}}"
  merge_lines=$((merge_lines + 1))
  check allow "pr-review-status merge line ${merge_lines} passes as printed" \
    "$(bash_payload "$cmd")"
  check deny  "pr-review-status merge line ${merge_lines} is denied without the prefix" \
    "$(bash_payload "${cmd//ALLOW_LITERAL_SHA=1 /}")"
done < <(grep -E 'echo ".*--match-head-commit \$\{head_sha\}' "$PRS")
if [ "$merge_lines" -lt 1 ]; then
  printf 'FAIL  %-52s found no --match-head-commit line to check\n' "pr-review-status merge lines exist"
  fail=$((fail + 1))
else
  pass=$((pass + 1))
fi

# --- malformed input must never block -----------------------------------
check allow "empty stdin"                      ""
check allow "not json"                         "wat"
check allow "unknown tool"                     '{"tool_name":"Read","tool_input":{"file_path":"/etc/hosts"}}'
check allow "bash with no command"             '{"tool_name":"Bash","tool_input":{}}'

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
