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
// A guard's own test suite is a gate too: its exit code is the evidence that
// a change to the guard is safe, and `[a-z0-9-]+\.sh` did not match
// `disk-watchdog.test.sh`, whose extra dot falls outside the class. Both
// guard directories are listed; the hook itself lives under `.claude`.
const GATE_HEAD =
  /^(cargo\s+(clippy|test|nextest|fmt|build|check)|(\.\/)?(scripts\/((gates|affected-tests|verify-dispatch-gates)\.sh|guards\/[a-z0-9.-]+\.sh)|\.claude\/guards\/[a-z0-9.-]+\.sh))\b/;
// Things that may legitimately precede a gate on the same command line.
// A bare assignment may precede a gate (`FOO=1 cargo test`). A command
// substitution assignment is stripped too, but as its own alternative, so the
// command INSIDE it is what gets matched: `out=$(scripts/guards/x.sh | tail
// -1)` is a masked guard whose exit code the caller then reads, and a single
// `[^\s]+` value swallowed `out=$(scripts/guards/x.sh` whole, leaving the
// pipe unseen. Ordered before the bare-assignment alternative, which is
// guarded against `$(` so it cannot re-swallow it.
const HARMLESS_PREFIX =
  /^(\s*(cd\s+[^&;|]+&&|[A-Za-z_][A-Za-z0-9_]*=\$\(|[A-Za-z_][A-Za-z0-9_]*=(?!\$\()[^\s]*|timeout(\s+-[A-Za-z-]+)*\s+\d+(\.\d+)?[smhd]?|time|nice(\s+-n\s*-?\d+)?|env|bash|sh|zsh|if|while|until|!)\s*)+/;
const MASKING_FILTER = /^\s*(tail|head|grep|rg|sed)\b/;
const MASKING_ECHO = /&&\s*echo\b/;

// zsh marks these read-only; assigning to one kills the enclosing loop with
// no output that looks like a failure.
const RESERVED_ASSIGN = /(^|[;&|(]|\bdo\b|\bthen\b|\blocal\b|\bexport\b)\s*(status|path|argv|PWD)=/;

// Command substitutions are checked as commands in their own right. Extending
// the harmless-prefix list instead only ever covers the spellings someone
// thought to enumerate: `out=$(gate | tail -1)` was covered and the same line
// with quotes around the substitution, or in backticks, was not, though all
// three run the gate and read the pipe's status. Single-quoted text is skipped
// because no substitution happens inside it.
function substitutionBodies(text) {
  const bodies = [];
  let sq = false;
  let dq = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === "\\") {
      i++;
      continue;
    }
    if (c === "'" && !dq) {
      sq = !sq;
      continue;
    }
    if (c === '"' && !sq) {
      dq = !dq;
      continue;
    }
    if (sq) continue;
    if (c === "$" && text[i + 1] === "(") {
      let depth = 1;
      let j = i + 2;
      for (; j < text.length && depth > 0; j++) {
        if (text[j] === "(") depth++;
        else if (text[j] === ")") depth--;
      }
      bodies.push(text.slice(i + 2, depth === 0 ? j - 1 : text.length));
      i = j - 1;
      continue;
    }
    if (c === "`") {
      const end = text.indexOf("`", i + 1);
      bodies.push(text.slice(i + 1, end === -1 ? text.length : end));
      i = end === -1 ? text.length : end;
    }
  }
  return bodies;
}

// A heredoc body is data, not shell. Statements split on newlines, so a
// document that QUOTES a piped gate is otherwise refused line by line: the
// commit message describing this rule could not be written by the tool that
// writes commit messages. Only the body is dropped, so a real gate elsewhere
// in the same command is still judged; exempting the whole command whenever
// it contains `<<` is what the reserved-name rule did, and that let a real
// `status=` through beside an unrelated heredoc.
function stripHeredocBodies(text) {
  const lines = text.split("\n");
  const kept = [];
  for (let i = 0; i < lines.length; i++) {
    kept.push(lines[i]);
    const opener = /<<(-?)\s*(["'])?([A-Za-z_][A-Za-z0-9_]*)\2?/g;
    const delims = [];
    let m;
    while ((m = opener.exec(lines[i])) !== null) {
      delims.push({ tag: m[3], dash: m[1] === "-" });
    }
    for (const d of delims) {
      i++;
      while (i < lines.length) {
        const line = d.dash ? lines[i].replace(/^\t+/, "") : lines[i];
        if (line === d.tag) break;
        i++;
      }
    }
  }
  return kept.join("\n");
}

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

// The command itself plus every command substitution nested inside it. The
// depth cap is a backstop against pathological input, not a real limit: two
// levels covers anything a session writes by hand.
function scanTexts(command) {
  const texts = [];
  const queue = [command];
  while (queue.length > 0 && texts.length < 64) {
    const text = queue.shift();
    texts.push(text);
    for (const body of substitutionBodies(text)) queue.push(body);
  }
  return texts;
}

function checkBash(rawCommand) {
  if (typeof rawCommand !== "string" || rawCommand === "") return;
  const command = stripHeredocBodies(rawCommand);

  for (const raw of scanTexts(command).flatMap(splitStatements)) {
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

    // Heredoc bodies are already gone, so this judges real shell only. The
    // condition here used to be `!command.includes("<<")`, which exempted a
    // reserved assignment sitting beside an unrelated heredoc.
    if (RESERVED_ASSIGN.test(stmt)) {
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
