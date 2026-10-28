import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { stripVTControlCharacters } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import { generateContract } from "./contract.js";
import { SCAFFOLD_FILES, scaffold, ScaffoldError } from "./index.js";
import { createOrderAgent } from "./template/agent.js";
import { PriceList } from "./template/fixture.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PACKAGE_DIR = path.resolve(HERE, "..", "..");
const SDK_DIR = path.resolve(PACKAGE_DIR, "..", "..");
const TSC = path.join(SDK_DIR, "node_modules", "typescript", "bin", "tsc");
const VITEST = path.join(SDK_DIR, "node_modules", "vitest", "vitest.mjs");
const BIN = path.join(PACKAGE_DIR, "bin", "ravel-agents-scaffold.js");

const EXPECTED_FILES = [
  ".gitignore",
  "README.md",
  "package.json",
  "src/agent.test.ts",
  "src/agent.ts",
  "src/client.ts",
  "src/contract.ts",
  "src/fixture.ts",
  "src/main.ts",
  "src/write-contract.ts",
  "ts-hooks.js",
  "tsconfig.json",
];

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "ravel-scaffold-"));
  dirs.push(dir);
  return dir;
}

/** Every file under `dir`, relative, sorted. */
async function listFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { recursive: true, withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => path.relative(dir, path.join(entry.parentPath, entry.name)).split(path.sep).join("/"))
    .sort();
}

async function snapshot(dir: string): Promise<Map<string, string>> {
  const files = new Map<string, string>();
  for (const file of await listFiles(dir)) files.set(file, await readFile(path.join(dir, file), "utf8"));
  return files;
}

/**
 * The environment with every `*_API_KEY` variable and vitest's own variables
 * removed, and colour off. vitest colours its output even into a pipe unless
 * NO_COLOR is set or it detects a coding agent in the environment, so without
 * this the summary line holds escape codes on a CI runner and not under an
 * agent.
 */
function strippedEnv(): NodeJS.ProcessEnv {
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) => !name.endsWith("_API_KEY") && !name.startsWith("VITEST") && name !== "FORCE_COLOR",
    ),
  );
  return { ...env, NO_COLOR: "1" };
}

/** vitest's `Tests` summary line, whitespace collapsed, or null when there is not exactly one. */
function testsLine(output: string): string | null {
  const lines = stripVTControlCharacters(output)
    .split("\n")
    .map((line) => line.trim().replace(/\s+/g, " "))
    .filter((line) => line.startsWith("Tests "));
  return lines.length === 1 ? (lines[0] ?? null) : null;
}

function run(args: readonly string[], cwd: string, env: NodeJS.ProcessEnv = strippedEnv()) {
  const result = spawnSync(process.execPath, args, { cwd, env, encoding: "utf8" });
  return { code: result.status, output: `${result.stdout}${result.stderr}`, stdout: result.stdout, stderr: result.stderr };
}

/** `entry` and every file it imports through a relative specifier, transitively. */
async function importClosure(dir: string, entry: string): Promise<string[]> {
  const seen = new Set<string>();
  const pending = [entry];
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (seen.has(file)) continue;
    seen.add(file);
    const source = await readFile(path.join(dir, file), "utf8");
    for (const match of source.matchAll(/from "(\.{1,2}\/[^"]+)\.js"/g)) {
      const specifier = match[1];
      if (specifier !== undefined) pending.push(path.posix.join(path.posix.dirname(file), `${specifier}.ts`));
    }
  }
  return [...seen].sort();
}

describe("scaffold", () => {
  it("scaffold output typechecks and its tests pass without an API key", async () => {
    const dir = path.join(await tempDir(), "orders");
    const result = await scaffold(dir);
    expect(result.files).toEqual(EXPECTED_FILES);
    expect(SCAFFOLD_FILES).toEqual(EXPECTED_FILES);
    expect(await listFiles(dir)).toEqual(EXPECTED_FILES);

    // No generated file names an API key, and nothing the generated test
    // loads reads the environment at all.
    const files = await snapshot(dir);
    for (const [file, content] of files) expect(content, file).not.toContain("API_KEY");
    const closure = await importClosure(dir, "src/agent.test.ts");
    expect(closure).toEqual(["src/agent.test.ts", "src/agent.ts", "src/client.ts", "src/contract.ts", "src/fixture.ts"]);
    for (const file of closure) expect(files.get(file), file).not.toContain("process.env");

    // The SDK's own install stands in for `npm install`, so the test needs no
    // network either. It does not cover npm resolving the generated `file:`
    // dependency: `npm install --offline` needs registry metadata that a cache
    // filled by `npm ci` does not hold.
    await symlink(path.join(SDK_DIR, "node_modules"), path.join(dir, "node_modules"), "dir");

    const typecheck = run([TSC, "--pretty", "false", "-p", "tsconfig.json"], dir);
    expect(typecheck.output).toBe("");
    expect(typecheck.code).toBe(0);

    const stripped = run([VITEST, "run"], dir);
    expect(stripped.code, stripped.output).toBe(0);
    expect(testsLine(stripped.output), stripped.output).toBe("Tests 4 passed (4)");

    const poison = { ...strippedEnv(), OPENAI_API_KEY: "poison", ANTHROPIC_API_KEY: "poison", RAVEL_AGENTS_API_KEY: "poison" };
    const poisoned = run([VITEST, "run"], dir, poison);
    expect(poisoned.code, poisoned.output).toBe(0);
    expect(testsLine(poisoned.output), poisoned.output).toBe("Tests 4 passed (4)");
  }, 300_000);

  it("a second scaffold into the same directory is refused and changes nothing", async () => {
    const dir = path.join(await tempDir(), "twice");
    await scaffold(dir);
    const before = await snapshot(dir);
    await expect(scaffold(dir)).rejects.toThrow(ScaffoldError);
    await expect(scaffold(dir)).rejects.toMatchObject({ code: "target_not_empty", dir });
    expect(await snapshot(dir)).toEqual(before);
  });

  it("a directory holding any other file is refused and nothing is written", async () => {
    const dir = await tempDir();
    await writeFile(path.join(dir, "notes.txt"), "mine\n");
    await expect(scaffold(dir)).rejects.toMatchObject({ name: "ScaffoldError", code: "target_not_empty" });
    expect(await listFiles(dir)).toEqual(["notes.txt"]);
    expect(await readFile(path.join(dir, "notes.txt"), "utf8")).toBe("mine\n");
  });

  it("a directory holding only an empty subdirectory is refused", async () => {
    const dir = await tempDir();
    await mkdir(path.join(dir, "src"));
    await expect(scaffold(dir)).rejects.toMatchObject({ code: "target_not_empty" });
    expect(await readdir(path.join(dir, "src"))).toEqual([]);
  });

  it("a path that is a file is refused", async () => {
    const file = path.join(await tempDir(), "file");
    await writeFile(file, "x");
    await expect(scaffold(file)).rejects.toMatchObject({ code: "target_not_a_directory", dir: file });
    expect(await readFile(file, "utf8")).toBe("x");
  });

  it("an existing empty directory is written into", async () => {
    const dir = await tempDir();
    await scaffold(dir);
    expect(await listFiles(dir)).toEqual(EXPECTED_FILES);
  });

  it("the generated package.json links this package and pins the workspace's versions", async () => {
    const dir = path.join(await tempDir(), "My Orders");
    await scaffold(dir);
    const manifest = JSON.parse(await readFile(path.join(dir, "package.json"), "utf8")) as {
      name: string;
      dependencies: Record<string, string>;
      devDependencies: Record<string, string>;
    };
    const agents = JSON.parse(await readFile(path.join(PACKAGE_DIR, "package.json"), "utf8")) as {
      dependencies: Record<string, string>;
    };
    const workspace = JSON.parse(await readFile(path.join(SDK_DIR, "package.json"), "utf8")) as {
      devDependencies: Record<string, string>;
    };
    expect(manifest.name).toBe("my-orders");
    expect(manifest.dependencies).toEqual({
      "@ravel/agents": `file:${path.relative(dir, PACKAGE_DIR).split(path.sep).join("/")}`,
      zod: agents.dependencies.zod,
    });
    expect(manifest.devDependencies).toEqual({
      "@types/node": workspace.devDependencies["@types/node"],
      typescript: workspace.devDependencies.typescript,
      vitest: workspace.devDependencies.vitest,
    });
  });

  it("the generated README runs the scaffold bin by its path, never by its bare name", async () => {
    const dir = await tempDir();
    await scaffold(dir);
    const readme = (await readFile(path.join(dir, "README.md"), "utf8")).replace(/\s*\n\s*/g, " ");
    expect(readme).toContain(
      "run `node packages/agents/bin/ravel-agents-scaffold.js <dir>`. Run the bin by its path; " +
        '`docs/guides/agents-sdk.md` in the Ravel repository, under "First run", says why.',
    );
    // The one mention is the path; no line names the bin bare.
    expect(readme.match(/ravel-agents-scaffold/g)).toHaveLength(1);
    expect(readme).not.toMatch(/(?<!bin\/)ravel-agents-scaffold/);
  });

  it("the template's contract.ts is the generator's output for the template agent", async () => {
    const checkedIn = await readFile(path.join(HERE, "template", "contract.ts"), "utf8");
    expect(checkedIn).toBe(generateContract(createOrderAgent(new PriceList()).order));
  });

  it("the bin writes a project, exits 1 on a non-empty directory and 64 on bad usage", async () => {
    const dir = path.join(await tempDir(), "from-bin");
    const written = run([BIN, dir], PACKAGE_DIR);
    expect(written.code).toBe(0);
    for (const file of EXPECTED_FILES) expect(written.stdout).toContain(`  ${file}\n`);
    expect(await listFiles(dir)).toEqual(EXPECTED_FILES);

    const again = run([BIN, dir], PACKAGE_DIR);
    expect(again.code).toBe(1);
    expect(again.stderr).toMatch(/^target_not_empty: /);

    const usage = run([BIN], PACKAGE_DIR);
    expect(usage.code).toBe(64);
    expect(usage.stderr).toBe("usage: ravel-agents-scaffold <dir>\n");
  }, 60_000);
});
