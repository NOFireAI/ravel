import { readFileSync } from "node:fs";
import { dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const srcRoot = resolve(packageRoot, "src");

/** Entry directories whose modules the client must never load. */
const SERVER_ONLY_DIRS = ["worker", "tools", "testing"];
/** Node built-ins a client bundle must never import, with or without `node:`. */
const SERVER_ONLY_BUILTINS = ["child_process", "fs", "net"];

interface ModuleGraph {
  /** Package-relative paths of every source module reached. */
  readonly files: string[];
  /** Every specifier that left the package (bare packages and built-ins). */
  readonly external: string[];
}

/**
 * Walk the import graph from `entry`, following relative specifiers and
 * self-references to `@ravel/agents/...`. Type-only imports are followed
 * too, which is stricter than a bundler needs. An unresolvable relative
 * specifier throws, so a graph the walker cannot read never passes.
 */
function moduleGraph(entry: string, read: (path: string) => string | undefined): ModuleGraph {
  const exportsMap = JSON.parse(read(resolve(packageRoot, "package.json")) ?? "{}") as {
    exports?: Record<string, { default: string }>;
  };
  const seen = new Set<string>();
  const external = new Set<string>();
  const queue = [entry];
  for (let file = queue.pop(); file !== undefined; file = queue.pop()) {
    if (seen.has(file)) continue;
    const text = read(file);
    if (text === undefined) throw new Error(`cannot read module ${file}`);
    seen.add(file);
    for (const { fileName: specifier } of ts.preProcessFile(text, true, true).importedFiles) {
      let target: string | undefined;
      if (specifier.startsWith(".")) {
        target = resolve(dirname(file), specifier.replace(/\.js$/, ".ts"));
      } else if (specifier === "@ravel/agents" || specifier.startsWith("@ravel/agents/")) {
        const key = "." + specifier.slice("@ravel/agents".length);
        const mapped = exportsMap.exports?.[key]?.default;
        if (mapped === undefined) throw new Error(`no export ${key} for ${specifier}`);
        target = resolve(packageRoot, mapped);
      } else {
        external.add(specifier);
        continue;
      }
      if (read(target) === undefined) throw new Error(`${file}: cannot resolve ${specifier}`);
      queue.push(target);
    }
  }
  return {
    files: [...seen].map((f) => relative(packageRoot, f).split(sep).join("/")).sort(),
    external: [...external].sort(),
  };
}

function serverOnlyViolations(graph: ModuleGraph): string[] {
  const files = graph.files.filter((f) =>
    SERVER_ONLY_DIRS.some((dir) => f.startsWith(`src/${dir}/`)),
  );
  const builtins = graph.external.filter((spec) => {
    const bare = spec.replace(/^node:/, "");
    return SERVER_ONLY_BUILTINS.some((b) => bare === b || bare.startsWith(`${b}/`));
  });
  return [...files, ...builtins];
}

function readSource(path: string): string | undefined {
  try {
    return readFileSync(path, "utf8");
  } catch {
    return undefined;
  }
}

describe("client bundle", () => {
  it("client entry imports no server-only module", () => {
    const graph = moduleGraph(resolve(srcRoot, "client", "index.ts"), readSource);
    // The walk really ran: the entry and the modules it re-exports are in it.
    expect(graph.files).toEqual(
      expect.arrayContaining(["src/client/index.ts", "src/errors.ts", "src/not-implemented.ts"]),
    );
    expect(serverOnlyViolations(graph)).toEqual([]);
  });

  it("the walker reports a server-only import at any depth", () => {
    const fixture = new Map<string, string>([
      [resolve(packageRoot, "package.json"), readSource(resolve(packageRoot, "package.json")) ?? ""],
      [resolve(srcRoot, "client", "index.ts"), 'export * from "../shared.js";'],
      [
        resolve(srcRoot, "shared.ts"),
        'import "node:fs/promises";\nimport type { X } from "@ravel/agents/worker";\nconst n = await import("net");',
      ],
      [resolve(srcRoot, "worker", "index.ts"), 'import "node:child_process";'],
    ]);
    const graph = moduleGraph(resolve(srcRoot, "client", "index.ts"), (p) => fixture.get(p));
    expect(serverOnlyViolations(graph)).toEqual([
      "src/worker/index.ts",
      "net",
      "node:child_process",
      "node:fs/promises",
    ]);
  });

  it("the walker fails closed on an unresolvable relative import", () => {
    const fixture = new Map<string, string>([
      [resolve(packageRoot, "package.json"), "{}"],
      [resolve(srcRoot, "client", "index.ts"), 'import "./missing.js";'],
    ]);
    expect(() =>
      moduleGraph(resolve(srcRoot, "client", "index.ts"), (p) => fixture.get(p)),
    ).toThrow(/cannot resolve \.\/missing\.js/);
  });
});
