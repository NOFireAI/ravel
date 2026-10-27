import { describe, expect, it } from "vitest";
import * as client from "./client/index.js";
import { NotImplementedError } from "./not-implemented.js";
import * as testing from "./testing/index.js";
import * as tools from "./tools/index.js";
import * as worker from "./worker/index.js";

const ENTRIES = { client, worker, tools, testing };

const STUBS: Record<keyof typeof ENTRIES, string[]> = {
  client: ["createClient"],
  worker: ["defineAgent", "message", "serveAgents"],
  tools: ["defineTool"],
  testing: ["createTestHarness"],
};

describe("declared-but-not-implemented entry points", () => {
  it.each(Object.entries(STUBS).flatMap(([entry, names]) => names.map((n) => [entry, n])))(
    "%s.%s throws NotImplementedError naming T12a",
    (entry, name) => {
      const fn = (ENTRIES[entry as keyof typeof ENTRIES] as Record<string, unknown>)[name];
      expect(typeof fn).toBe("function");
      let thrown: unknown;
      try {
        (fn as (arg: unknown) => unknown)({});
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toBeInstanceOf(NotImplementedError);
      const err = thrown as NotImplementedError;
      expect(err.code).toBe("NotImplemented");
      expect(err.task).toBe("T12a");
      expect(err.symbol).toBe(name);
      expect(err.message).toContain("T12a");
    },
  );

  it("the stub list covers every exported function", () => {
    for (const [entry, module] of Object.entries(ENTRIES)) {
      const functions = Object.entries(module as Record<string, unknown>)
        .filter(
          ([, v]) =>
            typeof v === "function" && !((v as { prototype?: unknown }).prototype instanceof Error),
        )
        .map(([k]) => k)
        .sort();
      expect(functions).toEqual([...STUBS[entry as keyof typeof ENTRIES]].sort());
    }
  });
});
