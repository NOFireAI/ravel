import { spawnSync } from "node:child_process";
import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { createAssistant } from "../../../../examples/assistant/assistant.js";
import { AssistantFixture } from "../../../../examples/assistant/fixture.js";
import type { AnyAgentDefinition } from "../types.js";
import { defineAgent } from "../worker/index.js";
import { ContractError, generateContract } from "./contract.js";

const SDK_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "..");
const TSC = path.join(SDK_DIR, "node_modules", "typescript", "bin", "tsc");
const ASSISTANT = path.join(SDK_DIR, "examples", "assistant", "assistant.js");

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

/**
 * A directory holding the assistant's generated contract and `check.ts`,
 * which fails to compile unless the contract's types equal the ones the
 * client derives from the agent definition itself.
 */
/** `check.ts` comparing a contract's types for `handler` with `agentType`'s, imported from `from`. */
function sameTypesCheck(from: string, agentType: string, handler: string): string {
  return [
    `import type { ${agentType} } from ${JSON.stringify(from)};`,
    'import type { OperationHandle, PayloadOf, ResultOf } from "@ravel/agents/client";',
    'import type { HandlerName, InputOf, OperationOf, OutputOf } from "./contract.js";',
    "",
    "type Same<X, Y> = [X] extends [Y] ? ([Y] extends [X] ? true : false) : false;",
    "",
    "export const checks: [",
    `  Same<HandlerName, ${JSON.stringify(handler)}>,`,
    `  Same<InputOf<${JSON.stringify(handler)}>, PayloadOf<${agentType}, ${JSON.stringify(handler)}>>,`,
    `  Same<OutputOf<${JSON.stringify(handler)}>, ResultOf<${agentType}, ${JSON.stringify(handler)}>>,`,
    `  Same<OperationOf<${JSON.stringify(handler)}>, OperationHandle<ResultOf<${agentType}, ${JSON.stringify(handler)}>>>,`,
    "] = [true, true, true, true];",
    "",
  ].join("\n");
}

/** An empty module project resolving packages from the SDK's install. */
async function tempProject(): Promise<string> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "ravel-contract-"));
  dirs.push(dir);
  await symlink(path.join(SDK_DIR, "node_modules"), path.join(dir, "node_modules"), "dir");
  await writeFile(path.join(dir, "package.json"), JSON.stringify({ type: "module" }));
  return dir;
}

async function contractProject(
  contract: string,
  check = sameTypesCheck(ASSISTANT, "Assistant", "ask"),
  dir?: string,
): Promise<string> {
  dir ??= await tempProject();
  await writeFile(path.join(dir, "contract.ts"), contract);
  await writeFile(path.join(dir, "check.ts"), check);
  await writeFile(
    path.join(dir, "tsconfig.json"),
    JSON.stringify({
      extends: path.join(SDK_DIR, "tsconfig.base.json"),
      compilerOptions: { noEmit: true },
      files: ["check.ts", "contract.ts"],
    }),
  );
  return dir;
}

function tsc(dir: string): { code: number | null; output: string } {
  // tsc colours its diagnostics when FORCE_COLOR is set, which splits "error TS".
  const run = spawnSync(process.execPath, [TSC, "--pretty", "false", "-p", dir], { encoding: "utf8" });
  return { code: run.status, output: `${run.stdout}${run.stderr}` };
}

const notes = defineAgent({
  name: "notes",
  version: "2",
  state: z.object({}),
  initialState: {},
  messages: (message) => ({
    put: message({
      payload: z.object({
        id: z.string(),
        "content-type": z.enum(["text", "json"]).default("text"),
        tags: z.array(z.string()).optional(),
      }),
      result: z.object({ stored: z.literal(true), size: z.number().int().nullable() }),
      handler: () => Promise.resolve({ stored: true as const, size: null }),
    }),
    drop: message({
      payload: z.object({ id: z.string() }),
      result: z.boolean(),
      handler: () => Promise.resolve(true),
    }),
  }),
});

/** An agent module with every record key form, written into a temp project and imported from there. */
const RECORDS_AGENT = `import { defineAgent } from "@ravel/agents/worker";
import { z } from "zod";

const keys = z.enum(["x", "y"]);

export const records = defineAgent({
  name: "records",
  version: "1",
  state: z.object({}),
  initialState: {},
  messages: (message) => ({
    go: message({
      payload: z.object({
        limits: z.record(keys, z.number()),
        caps: z.partialRecord(keys, z.number()),
        one: z.record(z.literal("x"), z.string()),
        free: z.record(z.string(), z.number()),
      }),
      result: z.record(keys, z.boolean()),
      handler: () => Promise.resolve({ x: true, y: false }),
    }),
  }),
});

export type Records = typeof records;
`;

function withResult(result: z.ZodType) {
  return defineAgent({
    name: "bad",
    version: "1",
    state: z.object({}),
    initialState: {},
    messages: (message) => ({
      go: message({ payload: z.object({}), result, handler: () => Promise.resolve(null as never) }),
    }),
  });
}

describe("generateContract", () => {
  it("the contract generated from the example assistant typechecks against the agent's own types", async () => {
    const contract = generateContract(createAssistant(new AssistantFixture()).assistant);
    const run = tsc(await contractProject(contract));
    expect(run.output).toBe("");
    expect(run.code).toBe(0);
  }, 120_000);

  it("a contract that disagrees with the agent fails that typecheck", async () => {
    const contract = generateContract(createAssistant(new AssistantFixture()).assistant);
    const wrong = contract.replace("escalate: boolean;", "escalate: string;");
    expect(wrong).not.toBe(contract);
    const run = tsc(await contractProject(wrong));
    expect(run.code).not.toBe(0);
    const errors = run.output.split("\n").filter((line) => line.includes("error TS"));
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/check\.ts\(\d+,\d+\): error TS2322:/);
  }, 120_000);

  it("emits handler names, input and output types and the operation handle type", () => {
    expect(generateContract(notes)).toBe(
      [
        '// Client types for the "notes" agent at version "2",',
        "// generated from its handler schemas by generateContract (@ravel/agents/contract).",
        "// Regenerate this file after changing a payload or result schema; do not edit it.",
        'import type { OperationHandle } from "@ravel/agents/client";',
        "",
        'export const AGENT_KIND = "notes";',
        'export const AGENT_VERSION = "2";',
        'export const HANDLER_NAMES = ["drop", "put"] as const;',
        "",
        "export type HandlerName = (typeof HANDLER_NAMES)[number];",
        "",
        "export interface Handlers {",
        "  readonly drop: {",
        "    readonly input: {",
        "      id: string;",
        "    };",
        "    readonly output: boolean;",
        "  };",
        "  readonly put: {",
        "    readonly input: {",
        "      id: string;",
        '      "content-type"?: "text" | "json" | undefined;',
        "      tags?: Array<string> | undefined;",
        "    };",
        "    readonly output: {",
        "      stored: true;",
        "      size: number | null;",
        "    };",
        "  };",
        "}",
        "",
        "/** What a client sends to `handler`. */",
        'export type InputOf<K extends HandlerName> = Handlers[K]["input"];',
        "",
        "/** What `handler`'s operation result resolves to. */",
        'export type OutputOf<K extends HandlerName> = Handlers[K]["output"];',
        "",
        "/** The handle `send` returns for `handler`. */",
        "export type OperationOf<K extends HandlerName> = OperationHandle<OutputOf<K>>;",
        "",
      ].join("\n"),
    );
  });

  it("an enum-keyed record is typed by its key union and typechecks against the agent's own types", async () => {
    const dir = await tempProject();
    await writeFile(path.join(dir, "records.ts"), RECORDS_AGENT);
    const { records } = (await import(path.join(dir, "records.ts"))) as { records: AnyAgentDefinition };
    const contract = generateContract(records);
    expect(contract).toContain(
      [
        "    readonly input: {",
        '      limits: Record<"x" | "y", number>;',
        '      caps: Partial<Record<"x" | "y", number>>;',
        '      one: Record<"x", string>;',
        "      free: Record<string, number>;",
        "    };",
      ].join("\n"),
    );
    // TypeScript relates `Record<string, V>` to `Record<"x" | "y", V>` both
    // ways, so the Same checks alone cannot see a widened key type; the
    // payloads below can.
    const check = [
      sameTypesCheck("./records.js", "Records", "go"),
      'export const full: InputOf<"go">["limits"] = { x: 1, y: 2 };',
      "// @ts-expect-error a record missing an enum key is refused, as zod refuses it",
      'export const short: InputOf<"go">["limits"] = { x: 1 };',
      'export const some: InputOf<"go">["caps"] = { x: 1 };',
      "// @ts-expect-error a key outside the enum is refused",
      'export const stray: InputOf<"go">["caps"] = { z: 1 };',
      "",
    ].join("\n");
    const run = tsc(await contractProject(contract, check, dir));
    expect(run.output).toBe("");
    expect(run.code).toBe(0);

    // The string-keyed form the generator wrote before the key union fails
    // the same check.
    const widened = contract.replace('limits: Record<"x" | "y", number>;', "limits: Record<string, number>;");
    expect(widened).not.toBe(contract);
    const wide = tsc(await contractProject(widened, check, dir));
    expect(wide.code).not.toBe(0);
    const errors = wide.output.split("\n").filter((line) => line.includes("error TS"));
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/check\.ts\(\d+,\d+\): error TS2578: Unused '@ts-expect-error' directive\./);
  }, 120_000);

  it("a record whose keys are constrained other than by an enum is refused naming the keyword", () => {
    for (const [result, keyword] of [
      [z.record(z.string().regex(/^a/), z.number()), "pattern"],
      [z.record(z.string().min(2), z.number()), "minLength"],
    ] as const) {
      let thrown: unknown;
      try {
        generateContract(withResult(result));
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toBeInstanceOf(ContractError);
      expect(thrown).toMatchObject({
        code: "unsupported_schema",
        at: "go.result",
        message: `go.result: JSON Schema keyword ${keyword} in propertyNames is not supported`,
      });
    }
  });

  it("an object with named properties and a typed catchall is refused; a loose object keeps its members", async () => {
    let thrown: unknown;
    try {
      generateContract(withResult(z.object({ a: z.string() }).catchall(z.number())));
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(ContractError);
    expect(thrown).toMatchObject({
      code: "unsupported_schema",
      at: "go.result",
      message: "go.result: an object with named properties and a catchall other than unknown is not supported",
    });

    // With no named properties a typed catchall is a plain record.
    expect(generateContract(withResult(z.object({}).catchall(z.number())))).toContain(
      "    readonly output: Record<string, number>;\n",
    );

    const loose = generateContract(withResult(z.object({ a: z.string() }).loose()));
    expect(loose).toContain(["    readonly output: {", "      a: string;", "    } & Record<string, unknown>;"].join("\n"));
    const check = [
      'import type { OutputOf } from "./contract.js";',
      'export const extra: OutputOf<"go"> = { a: "x", b: 1 };',
      "// @ts-expect-error a named member keeps its own type",
      'export const wrong: OutputOf<"go"> = { a: 1 };',
      "",
    ].join("\n");
    const run = tsc(await contractProject(loose, check));
    expect(run.output).toBe("");
    expect(run.code).toBe(0);
  }, 120_000);

  it("an empty union is typed never", () => {
    expect(generateContract(withResult(z.union([])))).toContain("    readonly output: never;\n");
  });

  it("a schema with no TypeScript form is refused with a ContractError naming the handler side", () => {
    type Node = { next?: Node | undefined };
    const node: z.ZodType<Node> = z.lazy(() => z.object({ next: node.optional() }));
    for (const result of [z.bigint(), z.date(), z.string().transform((s) => s.length), node]) {
      let thrown: unknown;
      try {
        generateContract(withResult(result));
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toBeInstanceOf(ContractError);
      expect(thrown).toMatchObject({ code: "unsupported_schema", at: "go.result" });
    }
  });
});
