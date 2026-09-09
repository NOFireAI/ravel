#!/usr/bin/env node
// PreToolUse guard: refuses tool calls that CLAUDE.md forbids, so the rule
// does not depend on a session choosing to remember it.
//
// Contract: reads the hook payload on stdin, prints a PreToolUse decision on
// stdout, exits 0. Any parse or filesystem error means allow: a guard that
// blocks on its own bug would kill unattended fleet tasks.

import { readFileSync, existsSync, statSync } from "node:fs";
import { dirname, resolve, sep } from "node:path";

const WAKEUP_FLOOR_SECONDS = 900;

function allow() {
  process.stdout.write("{}");
  process.exit(0);
}

function deny(reason) {
  process.stdout.write(
    JSON.stringify({
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "deny",
        permissionDecisionReason: reason,
      },
    }),
  );
  process.exit(0);
}

function readStdin() {
  try {
    return JSON.parse(readFileSync(0, "utf8"));
  } catch {
    return null;
  }
}

// --- Bash rules ---------------------------------------------------------

// Commands whose exit code is a gate. Matched only in command position, so
// a gate name quoted inside a grep pattern is not a gate.
const GATE_HEAD = /^(cargo\s+(clippy|test|nextest|fmt|build|check)|(\.\/)?scripts\/(gates|affected-tests|verify-dispatch-gates)\.sh)\b/;
// Things that may legitimately precede a gate on the same command line.
const HARMLESS_PREFIX = /^(\s*(cd\s+[^&;|]+&&|[A-Za-z_][A-Za-z0-9_]*=[^\s]+|time|nice(\s+-n\s*-?\d+)?|env|bash|sh|zsh)\s*)+/;
const MASKING_FILTER = /^\s*(tail|head|grep|rg|sed)\b/;
const MASKING_ECHO = /&&\s*echo\b/;

// zsh marks these read-only; assigning to one kills the enclosing loop with
// no output that looks like a failure.
const RESERVED_ASSIGN = /(^|[;&|(]|\bdo\b|\bthen\b|\blocal\b|\bexport\b)\s*(status|path|argv|PWD)=/;

function splitStatements(command) {
  // Rough statement split. Over-splitting only weakens a rule; it never
  // invents a violation, because each rule needs its whole pattern in one
  // fragment.
  return command.split(/\n|;/);
}

// Split on a real pipe, leaving `||` alone.
function splitPipeline(stmt) {
  return stmt.split(/\|(?!\|)/);
}

function startsWithGate(fragment) {
  return GATE_HEAD.test(fragment.replace(HARMLESS_PREFIX, ""));
}

function checkBash(command) {
  if (typeof command !== "string" || command === "") return;

  for (const raw of splitStatements(command)) {
    const stmt = raw.trim();
    if (stmt === "") continue;

    const stages = splitPipeline(stmt);
    if (stages.length > 1 && startsWithGate(stages[0].trim())) {
      const filtered = stages.slice(1).some((s) => MASKING_FILTER.test(s));
      if (filtered) {
        deny(
          "A gate piped into tail/head/grep/rg/sed reports the pipe's exit " +
            "code, not the gate's. Run the gate on its own and read its " +
            "output, or write the output to a file and grep the file " +
            "afterwards.",
        );
      }
    }

    if (startsWithGate(stmt) && MASKING_ECHO.test(stmt)) {
      deny(
        "`&& echo MARKER` after a gate masks the gate's exit code. Run the " +
          "gate alone and check its status directly.",
      );
    }

    // A heredoc body is not shell, and scratchpad heredocs are allowed.
    if (!command.includes("<<") && RESERVED_ASSIGN.test(stmt)) {
      deny(
        "zsh reserves status, path, argv and PWD. Assigning to one fails " +
          "with `read-only variable` and silently kills the enclosing " +
          "loop. Use a different name (rc, target_path, args).",
      );
    }
  }
}

// --- Edit/Write rules ---------------------------------------------------

function gitDirFor(startPath) {
  let dir = existsSync(startPath) && statSync(startPath).isDirectory()
    ? startPath
    : dirname(startPath);
  for (let i = 0; i < 64; i += 1) {
    const candidate = resolve(dir, ".git");
    if (existsSync(candidate)) return { repoRoot: dir, gitPath: candidate };
    const parent = dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
  return null;
}

// A dispatched fleet clone IS the isolated workspace, and CLAUDE.md exempts
// it. Two independent signals, because a false block there loses a task:
// the clone lives under the fleet work root, and it hosts no linked
// worktrees of its own.
function isExemptCheckout(repoRoot, gitPath) {
  if (process.env.RAVEL_GUARD_ALLOW_PRIMARY === "1") return true;
  const normalized = repoRoot.split(sep).join("/");
  if (normalized.includes("/fleet/work/") || normalized.includes("/var/lib/fleet")) {
    return true;
  }
  return !existsSync(resolve(gitPath, "worktrees"));
}

function checkFileWrite(filePath) {
  if (typeof filePath !== "string" || filePath === "") return;
  const found = gitDirFor(resolve(filePath));
  if (!found) return; // outside any repo: scratchpad, /tmp, home dotfiles
  const { repoRoot, gitPath } = found;

  // A linked worktree records .git as a file pointing at the real git dir.
  if (!statSync(gitPath).isDirectory()) return;

  if (isExemptCheckout(repoRoot, gitPath)) return;

  deny(
    `${repoRoot} is the primary checkout, and another session can hold ` +
      "in-flight state there. Create a worktree first " +
      "(`git worktree add -b <branch> ../<name> origin/main`) and edit " +
      "inside it. This rule has no doc-only or one-file exception.",
  );
}

// --- ScheduleWakeup rule ------------------------------------------------

function checkWakeup(input) {
  if (input?.stop === true) return;
  const delay = input?.delaySeconds;
  if (typeof delay !== "number") return;
  if (delay < WAKEUP_FLOOR_SECONDS) {
    deny(
      `delaySeconds ${delay} is below the ${WAKEUP_FLOOR_SECONDS}s floor. ` +
        "Each wakeup re-reads the whole session context, and 56% of them " +
        "in the last review found nothing. Arm a Monitor on the event " +
        "instead, and keep the wakeup as a long fallback.",
    );
  }
}

// --- entry --------------------------------------------------------------

try {
  const payload = readStdin();
  if (!payload) allow();

  const tool = payload.tool_name;
  const input = payload.tool_input ?? {};

  if (tool === "Bash") checkBash(input.command);
  else if (tool === "Write" || tool === "Edit" || tool === "MultiEdit") {
    checkFileWrite(input.file_path);
  } else if (tool === "ScheduleWakeup") checkWakeup(input);
} catch {
  // fall through to allow
}

allow();
