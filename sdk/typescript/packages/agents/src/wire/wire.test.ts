import { readdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import * as wire from "./index.js";

const fixtureDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../../fixtures");
const fixtureStems = readdirSync(fixtureDir)
  .filter((f) => f.endsWith(".json"))
  .map((f) => f.slice(0, -".json".length))
  .sort();

function read(stem: string): string {
  return readFileSync(resolve(fixtureDir, `${stem}.json`), "utf8");
}

/** Every exported request, response or error schema: one wire body each. */
function bodySchemas(): [string, z.ZodType][] {
  return Object.entries(wire as Record<string, unknown>).filter(
    (entry): entry is [string, z.ZodType] =>
      entry[1] instanceof z.ZodType && /(Request|Response|^ErrorBody)$/.test(entry[0]),
  );
}

describe("wire fixtures", () => {
  it("every fixture has a schema", () => {
    expect(fixtureStems.length).toBeGreaterThan(0);
    expect(Object.keys(wire.WIRE_FIXTURES).sort()).toEqual(fixtureStems);
  });

  it("every body schema has a fixture", () => {
    const covered = new Set<z.ZodType>(Object.values(wire.WIRE_FIXTURES));
    const bodies = bodySchemas();
    expect(bodies.length).toBeGreaterThan(20);
    expect(bodies.filter(([, schema]) => !covered.has(schema)).map(([name]) => name)).toEqual([]);
  });

  it.each(fixtureStems)("%s decodes and re-encodes to the same bytes", (stem) => {
    const schema = (wire.WIRE_FIXTURES as Record<string, z.ZodType>)[stem];
    if (schema === undefined) throw new Error(`no schema for ${stem}`);
    const text = read(stem);
    const decoded = wire.decodeWire(schema, JSON.parse(text));
    const encoded = wire.encodeWire(schema, decoded);
    expect(JSON.stringify(encoded, null, 2)).toBe(text.replace(/\n$/, ""));
  });

  it("a nanosecond timestamp past 2^53 decodes exactly", () => {
    const decoded = wire.decodeWire(
      wire.ResultResponse,
      JSON.parse(read("result_response_nanosecond")),
    );
    expect(decoded.result?.finished_unix_ns).toBe(1767225600123456789n);
  });

  it("a timestamp sent as a JSON number is refused", () => {
    const body = JSON.parse(read("result_response")) as { result: Record<string, unknown> };
    body.result.finished_unix_ns = 1767225600000000000;
    expect(() => wire.decodeWire(wire.ResultResponse, body)).toThrow(z.ZodError);
  });

  it("a timestamp outside i64 is refused on decode and encode", () => {
    const body = JSON.parse(read("result_response")) as { result: Record<string, unknown> };
    body.result.finished_unix_ns = "9223372036854775807";
    expect(wire.decodeWire(wire.ResultResponse, body).result?.finished_unix_ns).toBe(2n ** 63n - 1n);
    body.result.finished_unix_ns = "-9223372036854775808";
    expect(wire.decodeWire(wire.ResultResponse, body).result?.finished_unix_ns).toBe(-(2n ** 63n));
    body.result.finished_unix_ns = "9223372036854775808";
    expect(() => wire.decodeWire(wire.ResultResponse, body)).toThrow(z.ZodError);
    body.result.finished_unix_ns = "-9223372036854775809";
    expect(() => wire.decodeWire(wire.ResultResponse, body)).toThrow(z.ZodError);
    const decoded = wire.decodeWire(wire.ResultResponse, JSON.parse(read("result_response")));
    const result = decoded.result === null ? null : { ...decoded.result, finished_unix_ns: 2n ** 63n };
    expect(() => wire.encodeWire(wire.ResultResponse, { ...decoded, result })).toThrow(z.ZodError);
  });

  it("state is null until the first committed turn", () => {
    const poll = wire.decodeWire(
      wire.WorkerPollResponse,
      JSON.parse(read("worker_poll_response")),
    );
    expect(poll.assignment?.state).toBeNull();
    const state = wire.decodeWire(
      wire.StateResponse,
      JSON.parse(read("state_response_uncommitted")),
    );
    expect(state.state).toBeNull();
    expect(state.state_version).toBe(0);
  });

  it("a u64 counter past the safe-integer range is refused, not rounded", () => {
    const body = JSON.parse(read("send_response")) as Record<string, unknown>;
    body.accept_seq = 2 ** 53;
    expect(() => wire.decodeWire(wire.SendResponse, body)).toThrow(z.ZodError);
  });
});
