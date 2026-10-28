/**
 * The typed-contract generator: from an agent definition's handler schemas,
 * emit a TypeScript module of client types (the handler names, each
 * handler's input and output type, and the operation handle type) that a
 * client imports instead of the agent definition itself.
 *
 * Input types are the payload schemas' input side (what a client sends,
 * before defaults apply) and output types are the result schemas' output
 * side (what `result()` resolves to), the same split as `PayloadOf` and
 * `ResultOf`.
 */
import { z } from "zod";
import type { AnyAgentDefinition, MessageDefinition } from "../types.js";

export type ContractErrorCode = "unsupported_schema";

/** A handler schema has no TypeScript form the generator can write. */
export class ContractError extends Error {
  override readonly name = "ContractError";
  readonly code: ContractErrorCode;
  /** `<handler>.payload` or `<handler>.result`. */
  readonly at: string;

  constructor(at: string, message: string) {
    super(`${at}: ${message}`);
    this.code = "unsupported_schema";
    this.at = at;
  }
}

type JsonSchema = Record<string, unknown>;

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

function propertyName(name: string): string {
  return IDENTIFIER.test(name) ? name : JSON.stringify(name);
}

function isSchema(value: unknown): value is JsonSchema {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function literal(value: unknown, at: string): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  throw new ContractError(at, `literal ${JSON.stringify(value)} has no TypeScript form`);
}

function union(members: readonly string[]): string {
  const unique = [...new Set(members)];
  return unique.length === 1 ? (unique[0] ?? "never") : unique.join(" | ");
}

const KEYWORDS_HANDLED = new Set([
  "$schema",
  "type",
  "properties",
  "required",
  "additionalProperties",
  "propertyNames",
  "items",
  "enum",
  "const",
  "anyOf",
  "oneOf",
  // Validation-only keywords: they narrow values, not the TypeScript type.
  "default",
  "description",
  "title",
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
  "multipleOf",
  "minLength",
  "maxLength",
  "pattern",
  "format",
  "minItems",
  "maxItems",
  "minProperties",
  "maxProperties",
]);

function typeOf(schema: JsonSchema, at: string, depth: number): string {
  for (const key of Object.keys(schema)) {
    if (!KEYWORDS_HANDLED.has(key)) {
      throw new ContractError(at, `JSON Schema keyword ${key} is not supported`);
    }
  }
  if ("const" in schema) return literal(schema.const, at);
  if (Array.isArray(schema.enum)) {
    return union(schema.enum.map((value) => literal(value, at)));
  }
  const alternatives = schema.anyOf ?? schema.oneOf;
  if (Array.isArray(alternatives)) {
    return union(
      alternatives.map((member) => {
        if (!isSchema(member)) throw new ContractError(at, "a union member is not a schema");
        return typeOf(member, at, depth);
      }),
    );
  }
  const types = schema.type;
  if (types === undefined) return "unknown";
  const list = Array.isArray(types) ? types : [types];
  return union(
    list.map((type) => {
      switch (type) {
        case "string":
          return "string";
        case "number":
        case "integer":
          return "number";
        case "boolean":
          return "boolean";
        case "null":
          return "null";
        case "array":
          return arrayType(schema, at, depth);
        case "object":
          return objectType(schema, at, depth);
        default:
          throw new ContractError(at, `JSON Schema type ${JSON.stringify(type)} is not supported`);
      }
    }),
  );
}

function arrayType(schema: JsonSchema, at: string, depth: number): string {
  const items = schema.items;
  if (items === undefined) return "unknown[]";
  if (!isSchema(items)) throw new ContractError(at, "array items is not a schema");
  return `Array<${typeOf(items, at, depth)}>`;
}

/**
 * The keys a record's `propertyNames` allows: every string, or the finite set
 * an `enum` or `const` lists. Any other constraint on the keys is refused,
 * since a wider key type would let a client send a payload zod rejects.
 */
function recordKeys(schema: JsonSchema, at: string): readonly string[] | null {
  const names = schema.propertyNames;
  if (names === undefined) return null;
  if (!isSchema(names)) throw new ContractError(at, "propertyNames is not a schema");
  for (const key of Object.keys(names)) {
    if (key !== "type" && key !== "enum" && key !== "const") {
      throw new ContractError(at, `JSON Schema keyword ${key} in propertyNames is not supported`);
    }
  }
  if (names.type !== undefined && names.type !== "string") {
    throw new ContractError(at, `propertyNames type ${JSON.stringify(names.type)} is not supported`);
  }
  const listed: unknown = "const" in names ? [names.const] : names.enum;
  if (listed === undefined) return null;
  if (!Array.isArray(listed) || listed.length === 0 || !listed.every((key) => typeof key === "string")) {
    throw new ContractError(at, "propertyNames enum is not a list of strings");
  }
  return listed;
}

function objectType(schema: JsonSchema, at: string, depth: number): string {
  const keys = recordKeys(schema, at);
  if (keys !== null) return enumRecordType(schema, keys, at, depth);
  const properties = schema.properties ?? {};
  if (!isSchema(properties)) throw new ContractError(at, "object properties is not a map");
  const required = new Set(Array.isArray(schema.required) ? schema.required : []);
  const extra = schema.additionalProperties;
  const indent = "  ".repeat(depth + 1);
  const lines = Object.entries(properties).map(([name, member]) => {
    if (!isSchema(member)) throw new ContractError(at, `property ${name} is not a schema`);
    const type = typeOf(member, at, depth + 1);
    // `undefined` is spelled out so the type matches zod's own optional
    // members under exactOptionalPropertyTypes.
    return required.has(name)
      ? `${indent}${propertyName(name)}: ${type};`
      : `${indent}${propertyName(name)}?: ${type} | undefined;`;
  });
  const shape = lines.length === 0 ? null : `{\n${lines.join("\n")}\n${"  ".repeat(depth)}}`;
  if (isSchema(extra)) {
    const record = `Record<string, ${typeOf(extra, at, depth)}>`;
    return shape === null ? record : `${shape} & ${record}`;
  }
  // zod types an object schema with no members as `Record<string, never>`
  // on both its input and its output side.
  return shape ?? "Record<string, never>";
}

/**
 * A record keyed by a finite key set: `Record<K, V>` when every key is
 * required (`z.record`), `Partial<Record<K, V>>` when none is
 * (`z.partialRecord`), as zod types them.
 */
function enumRecordType(schema: JsonSchema, keys: readonly string[], at: string, depth: number): string {
  const extra = schema.additionalProperties;
  if (schema.properties !== undefined || !isSchema(extra)) {
    throw new ContractError(at, "propertyNames with an enum is supported only on a record");
  }
  const required = new Set(Array.isArray(schema.required) ? schema.required : []);
  const record = `Record<${union(keys.map((key) => JSON.stringify(key)))}, ${typeOf(extra, at, depth)}>`;
  const listed = keys.filter((key) => required.has(key)).length;
  if (listed === keys.length) return record;
  if (listed === 0) return `Partial<${record}>`;
  throw new ContractError(at, "a record requiring only some of its enum keys is not supported");
}

function jsonSchema(schema: z.ZodType, io: "input" | "output", at: string): JsonSchema {
  try {
    return z.toJSONSchema(schema, { io, unrepresentable: "throw", cycles: "throw" });
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new ContractError(at, `schema has no JSON Schema form: ${reason}`);
  }
}

/**
 * The contract module's source for `agent`. The output depends on nothing
 * but the definition, so regenerating an unchanged agent writes the same
 * bytes. Throws {@link ContractError} for a schema with no TypeScript form
 * (a `bigint`, a `Date`, a transform in a result schema, a recursive
 * schema, a record whose keys are constrained other than by an enum).
 */
export function generateContract(agent: AnyAgentDefinition): string {
  const handlers = Object.keys(agent.messages).sort();
  const entries = handlers.map((handler) => {
    const definition: MessageDefinition | undefined = agent.messages[handler];
    if (definition === undefined) throw new ContractError(handler, "handler is not defined");
    const input = typeOf(
      jsonSchema(definition.payload, "input", `${handler}.payload`),
      `${handler}.payload`,
      2,
    );
    const output = typeOf(
      jsonSchema(definition.result, "output", `${handler}.result`),
      `${handler}.result`,
      2,
    );
    return `  readonly ${propertyName(handler)}: {\n    readonly input: ${input};\n    readonly output: ${output};\n  };`;
  });
  const names = handlers.map((handler) => JSON.stringify(handler)).join(", ");
  return [
    `// Client types for the ${JSON.stringify(agent.name)} agent at version ${JSON.stringify(agent.version)},`,
    "// generated from its handler schemas by generateContract (@ravel/agents/contract).",
    "// Regenerate this file after changing a payload or result schema; do not edit it.",
    'import type { OperationHandle } from "@ravel/agents/client";',
    "",
    `export const AGENT_KIND = ${JSON.stringify(agent.name)};`,
    `export const AGENT_VERSION = ${JSON.stringify(agent.version)};`,
    `export const HANDLER_NAMES = [${names}] as const;`,
    "",
    "export type HandlerName = (typeof HANDLER_NAMES)[number];",
    "",
    "export interface Handlers {",
    ...entries,
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
  ].join("\n");
}
