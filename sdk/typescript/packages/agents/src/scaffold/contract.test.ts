import { spawnSync } from "node:child_process";
import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { createAssistant } from "../../../../examples/assistant/assistant.js";
import { AssistantFixture } from "../../../../examples/assistant/fixture.js";
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
async function contractProject(contract: string): Promise<string> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "ravel-contract-"));
  dirs.push(dir);
  await symlink(path.join(SDK_DIR, "node_modules"), path.join(dir, "node_modules"), "dir");
  await writeFile(path.join(dir, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(path.join(dir, "contract.ts"), contract);
  await writeFile(
    path.join(dir, "check.ts"),
    [
      `import type { Assistant } from ${JSON.stringify(ASSISTANT)};`,
      'import type { OperationHandle, PayloadOf, ResultOf } from "@ravel/agents/client";',
      'import type { HandlerName, InputOf, OperationOf, OutputOf } from "./contract.js";',
      "",
      "type Same<X, Y> = [X] extends [Y] ? ([Y] extends [X] ? true : false) : false;",
      "",
      "export const checks: [",
      '  Same<HandlerName, "ask">,',
      '  Same<InputOf<"ask">, PayloadOf<Assistant, "ask">>,',
      '  Same<OutputOf<"ask">, ResultOf<Assistant, "ask">>,',
      '  Same<OperationOf<"ask">, OperationHandle<ResultOf<Assistant, "ask">>>,',
      "] = [true, true, true, true];",
      "",
    ].join("\n"),
  );
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
  const run = spawnSync(process.execPath, [TSC, "-p", dir], { encoding: "utf8" });
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
