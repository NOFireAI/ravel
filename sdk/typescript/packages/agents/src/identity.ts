/**
 * The derivations of `crates/ravel-agents-core/src/identity.rs`: BLAKE3 over
 * the domain tag and length-prefixed parts. The worker uses the step input
 * hash to detect replay drift; the test harness uses the rest to mint the ids
 * a server would. `identity.test.ts` pins each against a value the Rust side
 * wrote into `sdk/fixtures/`.
 */
import { InvalidStepInput } from "./errors.js";

const IV = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);
const MSG_PERMUTATION = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8];
const CHUNK_START = 1;
const CHUNK_END = 2;
const PARENT = 4;
const ROOT = 8;
const BLOCK_LEN = 64;
const CHUNK_LEN = 1024;

function rotr(x: number, n: number): number {
  return ((x >>> n) | (x << (32 - n))) >>> 0;
}

function at(a: Uint32Array, i: number): number {
  return a[i] ?? 0;
}

function g(s: Uint32Array, a: number, b: number, c: number, d: number, mx: number, my: number): void {
  s[a] = (at(s, a) + at(s, b) + mx) >>> 0;
  s[d] = rotr(at(s, d) ^ at(s, a), 16);
  s[c] = (at(s, c) + at(s, d)) >>> 0;
  s[b] = rotr(at(s, b) ^ at(s, c), 12);
  s[a] = (at(s, a) + at(s, b) + my) >>> 0;
  s[d] = rotr(at(s, d) ^ at(s, a), 8);
  s[c] = (at(s, c) + at(s, d)) >>> 0;
  s[b] = rotr(at(s, b) ^ at(s, c), 7);
}

function compress(
  cv: Uint32Array,
  block: Uint32Array,
  counter: number,
  blockLen: number,
  flags: number,
): Uint32Array {
  const s = new Uint32Array(16);
  s.set(cv, 0);
  s.set(IV.subarray(0, 4), 8);
  s[12] = counter >>> 0;
  s[13] = Math.floor(counter / 0x100000000) >>> 0;
  s[14] = blockLen;
  s[15] = flags;
  let m = Uint32Array.from(block);
  for (let round = 0; round < 7; round++) {
    g(s, 0, 4, 8, 12, at(m, 0), at(m, 1));
    g(s, 1, 5, 9, 13, at(m, 2), at(m, 3));
    g(s, 2, 6, 10, 14, at(m, 4), at(m, 5));
    g(s, 3, 7, 11, 15, at(m, 6), at(m, 7));
    g(s, 0, 5, 10, 15, at(m, 8), at(m, 9));
    g(s, 1, 6, 11, 12, at(m, 10), at(m, 11));
    g(s, 2, 7, 8, 13, at(m, 12), at(m, 13));
    g(s, 3, 4, 9, 14, at(m, 14), at(m, 15));
    const prev = m;
    m = Uint32Array.from(MSG_PERMUTATION, (p) => at(prev, p));
  }
  for (let i = 0; i < 8; i++) {
    s[i] = at(s, i) ^ at(s, i + 8);
    s[i + 8] = at(s, i + 8) ^ at(cv, i);
  }
  return s;
}

function blockWords(bytes: Uint8Array): Uint32Array {
  const padded = new Uint8Array(BLOCK_LEN);
  padded.set(bytes);
  const view = new DataView(padded.buffer);
  return Uint32Array.from({ length: 16 }, (_, i) => view.getUint32(i * 4, true));
}

/** The inputs of a not-yet-finalized compression, so the root flag can be added. */
interface Output {
  readonly cv: Uint32Array;
  readonly block: Uint32Array;
  readonly counter: number;
  readonly blockLen: number;
  readonly flags: number;
}

function chainingValue(o: Output): Uint32Array {
  return compress(o.cv, o.block, o.counter, o.blockLen, o.flags).subarray(0, 8);
}

function chunkOutput(chunk: Uint8Array, counter: number): Output {
  let cv: Uint32Array = IV;
  const blocks = Math.max(1, Math.ceil(chunk.length / BLOCK_LEN));
  for (let i = 0; i < blocks - 1; i++) {
    const flags = i === 0 ? CHUNK_START : 0;
    const block = blockWords(chunk.subarray(i * BLOCK_LEN, (i + 1) * BLOCK_LEN));
    cv = compress(cv, block, counter, BLOCK_LEN, flags).subarray(0, 8);
  }
  const last = chunk.subarray((blocks - 1) * BLOCK_LEN);
  return {
    cv,
    block: blockWords(last),
    counter,
    blockLen: last.length,
    flags: (blocks === 1 ? CHUNK_START : 0) | CHUNK_END,
  };
}

function parentOutput(left: Uint32Array, right: Uint32Array): Output {
  const block = new Uint32Array(16);
  block.set(left, 0);
  block.set(right, 8);
  return { cv: IV, block, counter: 0, blockLen: BLOCK_LEN, flags: PARENT };
}

/** BLAKE3 with the default 32-byte output. */
export function blake3(input: Uint8Array): Uint8Array {
  const chunks = Math.max(1, Math.ceil(input.length / CHUNK_LEN));
  const stack: Uint32Array[] = [];
  for (let i = 0; i < chunks - 1; i++) {
    let cv = chainingValue(chunkOutput(input.subarray(i * CHUNK_LEN, (i + 1) * CHUNK_LEN), i));
    let total = i + 1;
    while ((total & 1) === 0) {
      const left = stack.pop();
      if (left === undefined) break;
      cv = chainingValue(parentOutput(left, cv));
      total >>>= 1;
    }
    stack.push(cv);
  }
  let output = chunkOutput(input.subarray((chunks - 1) * CHUNK_LEN), chunks - 1);
  for (let left = stack.pop(); left !== undefined; left = stack.pop()) {
    output = parentOutput(left, chainingValue(output));
  }
  const words = compress(output.cv, output.block, output.counter, output.blockLen, output.flags | ROOT);
  const out = new Uint8Array(32);
  const view = new DataView(out.buffer);
  for (let i = 0; i < 8; i++) view.setUint32(i * 4, at(words, i), true);
  return out;
}

const encoder = new TextEncoder();

/** `identity::DOMAIN`. */
export const DOMAIN = "ravel-agent-v1";

function u64le(value: bigint): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, value, true);
  return out;
}

export function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** `identity::derive`: the domain, then each part prefixed by its u64le length. */
export function derive(purpose: string, parts: readonly (Uint8Array | string)[]): Uint8Array {
  const all = [encoder.encode(purpose), ...parts.map((p) => (typeof p === "string" ? encoder.encode(p) : p))];
  const domain = encoder.encode(DOMAIN);
  const size = domain.length + all.reduce((n, p) => n + 8 + p.length, 0);
  const buf = new Uint8Array(size);
  buf.set(domain, 0);
  let offset = domain.length;
  for (const part of all) {
    buf.set(u64le(BigInt(part.length)), offset);
    buf.set(part, offset + 8);
    offset += 8 + part.length;
  }
  return blake3(buf);
}

/** `identity::step_input_hash`, as lowercase hex. */
export function stepInputHash(toolName: string, toolVersion: string, input: Uint8Array): string {
  return toHex(derive("step-input", [toolName, toolVersion, input]));
}

/** `identity::step_id`, as lowercase hex. */
export function stepId(incarnation: number, acceptSeq: number, stepKey: string): string {
  return toHex(
    derive("step", [u64le(BigInt(incarnation)), u64le(BigInt(acceptSeq)), stepKey]).subarray(0, 16),
  );
}

/** `identity::agent_id`: the domain, then kind and key each prefixed by its u16le length. */
export function agentId(kind: string, key: string): string {
  const k = encoder.encode(kind);
  const v = encoder.encode(key);
  const domain = encoder.encode(DOMAIN);
  const buf = new Uint8Array(domain.length + 4 + k.length + v.length);
  const view = new DataView(buf.buffer);
  buf.set(domain, 0);
  view.setUint16(domain.length, k.length, true);
  buf.set(k, domain.length + 2);
  view.setUint16(domain.length + 2 + k.length, v.length, true);
  buf.set(v, domain.length + 4 + k.length);
  return toHex(blake3(buf).subarray(0, 16));
}

/** `identity::effect_id`, as lowercase hex. */
export function effectId(incarnation: number, acceptSeq: number, intentKey: string): string {
  return toHex(
    derive("effect", [u64le(BigInt(incarnation)), u64le(BigInt(acceptSeq)), intentKey]).subarray(0, 16),
  );
}

/** `identity::turn_id`, as lowercase hex. */
export function turnId(incarnation: number, acceptSeq: number, epoch: number, failedAttempts: number): string {
  const attempts = new Uint8Array(4);
  new DataView(attempts.buffer).setUint32(0, failedAttempts, true);
  return toHex(
    derive("turn", [
      u64le(BigInt(incarnation)),
      u64le(BigInt(acceptSeq)),
      u64le(BigInt(epoch)),
      attempts,
    ]).subarray(0, 16),
  );
}

/** `identity::request_hash`, as lowercase hex. */
export function requestHash(handler: string, payload: Uint8Array): string {
  return toHex(derive("request", [handler, payload]));
}

function notJson(path: string, what: string): InvalidStepInput {
  return new InvalidStepInput(`${path} is ${what}, not plain JSON`);
}

function canonical(value: unknown, path: string): string {
  switch (typeof value) {
    case "string":
    case "boolean":
      return JSON.stringify(value);
    case "number":
      if (!Number.isFinite(value)) throw notJson(path, String(value));
      return JSON.stringify(value);
    case "object":
      break;
    default:
      throw notJson(path, `a ${typeof value}`);
  }
  if (value === null) return "null";
  if (Array.isArray(value)) {
    return `[${value.map((v: unknown, i) => canonical(v, `${path}[${String(i)}]`)).join(",")}]`;
  }
  const proto: unknown = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    const name = (value as { constructor?: { name?: unknown } }).constructor?.name;
    throw notJson(path, `a ${typeof name === "string" ? name : "class instance"}`);
  }
  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0,
  );
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonical(v, `${path}.${k}`)}`).join(",")}}`;
}

/**
 * JSON with object keys sorted at every depth: the bytes the SDK hashes a
 * step input over, so two equal inputs hash equal whatever their key order.
 * A value `JSON.stringify` would encode lossily or not at all (a `Date`,
 * `Map`, `Set`, `bigint`, function, non-finite number, `undefined`, or any
 * other class instance) is refused with `InvalidStepInput`, so two different
 * inputs cannot hash equal by both encoding as `{}`.
 */
export function canonicalJson(value: unknown): string {
  return canonical(value, "value");
}

export function canonicalBytes(value: unknown): Uint8Array {
  return encoder.encode(canonicalJson(value));
}

/**
 * The JSON form of a schema's output, as `JSON.stringify` would produce it:
 * every `toJSON` method applied (a `Date` becomes its ISO string), an
 * object member whose value is `undefined` dropped, and an `undefined`
 * array item written as `null`, so `{ a: undefined }` and `{}` are the same
 * input, as they are on the wire. Anything else is left in place for
 * {@link canonicalJson} to accept or refuse; a top-level `undefined` stays
 * refused there.
 */
export function jsonForm(value: unknown, key = ""): unknown {
  if (typeof value !== "object" || value === null) return value;
  const toJSON = (value as { toJSON?: unknown }).toJSON;
  if (typeof toJSON === "function") return jsonForm((toJSON as (key: string) => unknown).call(value, key), key);
  if (Array.isArray(value)) {
    return value.map((v: unknown, i) => {
      const item = jsonForm(v, String(i));
      return item === undefined ? null : item;
    });
  }
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .map(([k, v]) => [k, jsonForm(v, k)] as const)
      .filter(([, v]) => v !== undefined),
  );
}
