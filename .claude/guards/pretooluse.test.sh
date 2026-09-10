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

# --- malformed input must never block -----------------------------------
check allow "empty stdin"                      ""
check allow "not json"                         "wat"
check allow "unknown tool"                     '{"tool_name":"Read","tool_input":{"file_path":"/etc/hosts"}}'
check allow "bash with no command"             '{"tool_name":"Bash","tool_input":{}}'

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
