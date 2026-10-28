import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { blake3, canonicalJson, stepId, stepInputHash, toHex } from "./identity.js";

const fixtures = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../fixtures");

// Written by the blake3 crate 1.8.5 (the version in Cargo.lock) over input
// byte i = i % 251, the official test-vector input; the lengths cross the
// block, chunk and tree boundaries.
const VECTORS: [number, string][] = [
  [0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"],
  [1, "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"],
  [63, "e9bc37a594daad83be9470df7f7b3798297c3d834ce80ba85d6e207627b7db7b"],
  [64, "4eed7141ea4a5cd4b788606bd23f46e212af9cacebacdc7d1f4c6dc7f2511b98"],
  [65, "de1e5fa0be70df6d2be8fffd0e99ceaa8eb6e8c93a63f2d8d1c30ecb6b263dee"],
  [1023, "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11"],
  [1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"],
  [1025, "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"],
  [2048, "e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a"],
  [2049, "5f4d72f40d7a5f82b15ca2b2e44b1de3c2ef86c426c95c1af0b6879522563030"],
  [3072, "b98cb0ff3623be03326b373de6b9095218513e64f1ee2edd2525c7ad1e5cffd2"],
  [3073, "7124b49501012f81cc7f11ca069ec9226cecb8a2c850cfe644e327d22d3e1cd3"],
  [4096, "015094013f57a5277b59d8475c0501042c0b642e531b0a1c8f58d2163229e969"],
  [4097, "9b4052b38f1c5fc8b1f9ff7ac7b27cd242487b3d890d15c96a1c25b8aa0fb995"],
  [5120, "9cadc15fed8b5d854562b26a9536d9707cadeda9b143978f319ab34230535833"],
  [8192, "aae792484c8efe4f19e2ca7d371d8c467ffb10748d8a5a1ae579948f718a2a63"],
  [31744, "62b6960e1a44bcc1eb1a611a8d6235b6b4b78f32e7abc4fb4c6cdcce94895c47"],
  [102400, "bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085"],
];

describe("identity", () => {
  it.each(VECTORS)("blake3 of %i bytes matches the Rust crate", (len, hex) => {
    const input = Uint8Array.from({ length: len }, (_, i) => i % 251);
    expect(toHex(blake3(input))).toBe(hex);
  });

  it("step id and step input hash match the worker_poll_response fixture", () => {
    // The Rust fixture writer derives these as step_id(1, 1, "price") and
    // step_input_hash("catalog.price", "1", b"A-1").
    const poll = JSON.parse(readFileSync(resolve(fixtures, "worker_poll_response.json"), "utf8")) as {
      assignment: { steps: { step_id: string; input_hash: string }[] };
    };
    const step = poll.assignment.steps[0];
    expect(stepId(1, 1, "price")).toBe(step?.step_id);
    expect(stepInputHash("catalog.price", "1", new TextEncoder().encode("A-1"))).toBe(
      step?.input_hash,
    );
  });

  it("canonical JSON sorts keys at every depth", () => {
    expect(canonicalJson({ b: [{ d: 1, c: 2 }], a: null })).toBe('{"a":null,"b":[{"c":2,"d":1}]}');
  });
});
