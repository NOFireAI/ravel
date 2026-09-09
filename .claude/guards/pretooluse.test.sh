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
check allow "git log into head"               "$(bash_payload 'git log --oneline | head -5')"
check allow "grepping a saved gate log"       "$(bash_payload 'grep -c FAILED /tmp/gate.log')"

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
