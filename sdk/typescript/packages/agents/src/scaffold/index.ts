/**
 * Scaffold entry: write a self-contained starter project for one agent into
 * an empty directory. The project holds one agent with one deterministic
 * local tool fixture, the client types generated from its handler schemas,
 * a typed client, a vitest suite that runs the agent against
 * `@ravel/agents/testing`, and the package.json, tsconfig and README to run
 * it. Its tests need no server, no network and no API key. Server-only.
 *
 * `ravel-agents-scaffold <dir>` (this package's bin) runs {@link scaffold}.
 */
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateContract } from "./contract.js";
import { createOrderAgent } from "./template/agent.js";
import { PriceList } from "./template/fixture.js";

export { ContractError, type ContractErrorCode, generateContract } from "./contract.js";

export type ScaffoldErrorCode = "target_not_empty" | "target_not_a_directory";

/** The target cannot take a scaffold; nothing was written. */
export class ScaffoldError extends Error {
  override readonly name = "ScaffoldError";
  readonly code: ScaffoldErrorCode;
  /** The target directory, resolved. */
  readonly dir: string;

  constructor(code: ScaffoldErrorCode, dir: string, message: string) {
    super(message);
    this.code = code;
    this.dir = dir;
  }
}

/** Every file a scaffold writes, relative to its directory, in write order. */
export const SCAFFOLD_FILES: readonly string[] = Object.freeze([
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
]);

/** Files copied verbatim from `template/` into the project's `src/`. */
const TEMPLATE_SOURCES = ["agent.test.ts", "agent.ts", "client.ts", "fixture.ts", "main.ts", "write-contract.ts"];

export interface ScaffoldOptions {
  /**
   * The `@ravel/agents` package directory the project depends on through a
   * relative `file:` link. Defaults to this package's own directory.
   */
  readonly agentsPackageDir?: string;
}

export interface ScaffoldResult {
  /** The target directory, resolved. */
  readonly dir: string;
  /** The files written, relative to `dir`: {@link SCAFFOLD_FILES}. */
  readonly files: readonly string[];
}

const here = path.dirname(fileURLToPath(import.meta.url));
const PACKAGE_DIR = path.resolve(here, "..", "..");
const WORKSPACE_DIR = path.resolve(PACKAGE_DIR, "..", "..");

interface PackageJson {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
  readonly engines?: Readonly<Record<string, string>>;
}

async function readPackage(file: string): Promise<PackageJson> {
  return JSON.parse(await readFile(file, "utf8")) as PackageJson;
}

function pinned(manifest: PackageJson, section: "dependencies" | "devDependencies", name: string): string {
  const version = manifest[section]?.[name];
  if (version === undefined) throw new Error(`scaffold: the SDK pins no ${name} in ${section}`);
  return version;
}

/** A package name from the directory's base name, as npm accepts it. */
function packageName(dir: string): string {
  const name = path
    .basename(dir)
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^[._-]+|-+$/g, "");
  return name === "" ? "ravel-agent" : name.slice(0, 214);
}

/** Refuses anything but an absent or empty directory; creates an absent one. */
async function claim(dir: string): Promise<void> {
  let entries: string[];
  try {
    const info = await stat(dir);
    if (!info.isDirectory()) {
      throw new ScaffoldError("target_not_a_directory", dir, `scaffold: ${dir} exists and is not a directory`);
    }
    entries = await readdir(dir);
  } catch (error) {
    if (error instanceof ScaffoldError) throw error;
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    await mkdir(dir, { recursive: true });
    return;
  }
  if (entries.length > 0) {
    const shown = entries.sort().slice(0, 5).join(", ");
    throw new ScaffoldError(
      "target_not_empty",
      dir,
      `scaffold: ${dir} is not empty (${shown}${entries.length > 5 ? ", ..." : ""}); scaffold writes only into an empty or absent directory and never overwrites`,
    );
  }
}

async function render(dir: string, options: ScaffoldOptions): Promise<Map<string, string>> {
  const agentsDir = path.resolve(options.agentsPackageDir ?? PACKAGE_DIR);
  const agentsManifest = await readPackage(path.join(PACKAGE_DIR, "package.json"));
  const workspaceManifest = await readPackage(path.join(WORKSPACE_DIR, "package.json"));
  const link = path.relative(dir, agentsDir).split(path.sep).join("/");
  const name = packageName(dir);
  const engines = workspaceManifest.engines?.node;

  const manifest = {
    name,
    version: "0.0.0",
    private: true,
    type: "module",
    ...(engines === undefined ? {} : { engines: { node: engines } }),
    scripts: {
      test: "vitest run",
      typecheck: "tsc -p tsconfig.json",
      contract: "node --experimental-transform-types --disable-warning=ExperimentalWarning --import ./ts-hooks.js src/write-contract.ts",
      start: "node --experimental-transform-types --disable-warning=ExperimentalWarning --import ./ts-hooks.js src/main.ts",
    },
    dependencies: {
      "@ravel/agents": `file:${link}`,
      zod: pinned(agentsManifest, "dependencies", "zod"),
    },
    devDependencies: {
      "@types/node": pinned(workspaceManifest, "devDependencies", "@types/node"),
      typescript: pinned(workspaceManifest, "devDependencies", "typescript"),
      vitest: pinned(workspaceManifest, "devDependencies", "vitest"),
    },
  };

  const tsconfig = {
    compilerOptions: {
      target: "ES2022",
      lib: ["ES2022"],
      module: "NodeNext",
      moduleResolution: "NodeNext",
      types: ["node"],
      strict: true,
      exactOptionalPropertyTypes: true,
      noUncheckedIndexedAccess: true,
      noImplicitOverride: true,
      noImplicitReturns: true,
      noFallthroughCasesInSwitch: true,
      noUnusedLocals: true,
      verbatimModuleSyntax: true,
      forceConsistentCasingInFileNames: true,
      skipLibCheck: true,
      noEmit: true,
    },
    include: ["src"],
  };

  const files = new Map<string, string>();
  files.set(".gitignore", "node_modules/\n");
  files.set("README.md", readme(name, link));
  files.set("package.json", `${JSON.stringify(manifest, null, 2)}\n`);
  for (const source of TEMPLATE_SOURCES) {
    files.set(`src/${source}`, await readFile(path.join(here, "template", source), "utf8"));
  }
  files.set("src/contract.ts", generateContract(createOrderAgent(new PriceList()).order));
  files.set("ts-hooks.js", await readFile(path.join(WORKSPACE_DIR, "examples", "ts-hooks.js"), "utf8"));
  files.set("tsconfig.json", `${JSON.stringify(tsconfig, null, 2)}\n`);
  return files;
}

function readme(name: string, link: string): string {
  return `# ${name}

A starter project for one Ravel agent, written by \`ravel-agents-scaffold\`.
It needs node 22.15 or later 22.x. Its tests run the agent against an
in-process fake coordinator: no server, no network and no API key.

\`\`\`sh
npm install
npm test            # the agent's tests, against @ravel/agents/testing
npm run typecheck   # tsc over src/
\`\`\`

## What is here

| File | What it holds |
|---|---|
| \`src/agent.ts\` | The \`order\` agent: \`checkout\` prices one line through the durable step \`price\` and adds it to the order's total; \`summary\` reads the total |
| \`src/fixture.ts\` | The \`catalog.price\` tool's fixture: a fixed price list in memory, with every lookup counted |
| \`src/contract.ts\` | The client types generated from the agent's handler schemas: handler names, each handler's input and output type, and the operation handle type |
| \`src/client.ts\` | A client for the agent typed by \`contract.ts\` alone |
| \`src/agent.test.ts\` | The tests: a checkout, an unknown SKU, a worker killed inside the price step, and the contract matching the agent |
| \`src/main.ts\` | Serves the agent against a running server and checks out one line |
| \`src/write-contract.ts\` | Rewrites \`contract.ts\` from the agent's handler schemas |
| \`ts-hooks.js\` | Lets node run the TypeScript sources directly for \`npm start\` and \`npm run contract\` |

\`@ravel/agents\` is not published. \`package.json\` links it from the SDK
checkout this project was generated from, at \`${link}\` relative to this
directory; moving either one breaks the link, and editing that path in
\`package.json\` and running \`npm install\` again mends it.

## Changing the agent

After changing a payload or result schema in \`src/agent.ts\`, run
\`npm run contract\` to regenerate \`src/contract.ts\`. Until you do, the test
\`contract.ts is the generator's output for the agent\` fails, and
\`npm run typecheck\` reports any caller the change breaks.

## Kill and recover in a test

\`a worker killed inside the price step is replaced and the turn commits
once\` holds the price lookup open, kills the worker with
\`harness.crashWorker()\`, and checks that the new worker ran the
\`at_least_once\` step again and that the turn committed once. The fake
coordinator keeps everything in process memory: it shows how the handler
behaves under replay, not what the server persists.

## Run it against a server

Start a server with agents turned on. Set \`RAVEL_AGENTS_TOKEN\` to a token
with the \`agents\` capability and \`RAVEL_WORKER_TOKEN\` to one with
\`agents_worker\`, and \`RAVEL_URL\` when the server is not at
\`http://127.0.0.1:4318\`, then run:

\`\`\`sh
npm start -- A-1 2
\`\`\`

It serves the agent with a worker, sends \`checkout\` for the SKU and
quantity given (default \`A-1\` and \`2\`) to a new order, and prints the
operation id, the line and the total.
`;
}

/**
 * Write the starter project into `dir`, creating it when absent. Refuses a
 * path that is not a directory or a directory that is not empty with a
 * {@link ScaffoldError}, before writing anything; never overwrites a file.
 */
export async function scaffold(dir: string, options: ScaffoldOptions = {}): Promise<ScaffoldResult> {
  const target = path.resolve(dir);
  const files = await render(target, options);
  await claim(target);
  for (const file of SCAFFOLD_FILES) {
    const content = files.get(file);
    if (content === undefined) throw new Error(`scaffold: no content rendered for ${file}`);
    const absolute = path.join(target, file);
    await mkdir(path.dirname(absolute), { recursive: true });
    await writeFile(absolute, content, { flag: "wx" });
  }
  return { dir: target, files: SCAFFOLD_FILES };
}
