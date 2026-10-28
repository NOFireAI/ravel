import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { COMPOSE_FILE, decide, plan, REPO_ROOT, serverBinary } from "./server.js";

const nothing = { exists: () => false, dockerCompose: () => false };
const everything = { exists: () => true, dockerCompose: () => true };

describe("e2e plan", () => {
  it("resolves the repository root to the directory holding the workspace Cargo.toml", () => {
    expect(readFileSync(join(REPO_ROOT, "Cargo.toml"), "utf8")).toMatch(/^\[workspace\]/m);
    expect(existsSync(join(REPO_ROOT, "sdk", "typescript", "packages", "agents", "e2e", "server.ts"))).toBe(true);
    expect(existsSync(COMPOSE_FILE)).toBe(true);
  });

  it("looks for the server under the repository's target unless told otherwise", () => {
    expect(serverBinary({})).toBe(join(REPO_ROOT, "target", "debug", "ravel-server"));
    expect(serverBinary({ CARGO_TARGET_DIR: "/t" })).toBe(join("/t", "debug", "ravel-server"));
    expect(serverBinary({ RAVEL_SERVER_BIN: "/bin/rs", CARGO_TARGET_DIR: "/t" })).toBe("/bin/rs");
  });

  it("names the resolved binary path when it is missing", () => {
    const target = plan({}, nothing);
    expect(target.missing).toBe(
      `ravel-server binary not found at ${join(REPO_ROOT, "target", "debug", "ravel-server")}; build it with ` +
        "`cargo build --locked -p ravel-server --features agents` or set RAVEL_SERVER_BIN",
    );
  });

  it("needs docker compose only for the rustfs store", () => {
    const noDocker = { exists: () => true, dockerCompose: () => false };
    expect(plan({}, noDocker).missing).toBeUndefined();
    expect(plan({ RAVEL_E2E_STORE: "rustfs" }, noDocker).missing).toMatch(/needs `docker compose`/);
    expect(plan({ RAVEL_E2E_STORE: "rustfs" }, everything)).toMatchObject({ store: "rustfs", missing: undefined });
    expect(plan({ RAVEL_E2E_STORE: "s3" }, everything).missing).toBe("RAVEL_E2E_STORE=s3 is neither memory nor rustfs");
  });

  it("skips on a missing prerequisite, and fails instead under RAVEL_E2E_REQUIRE=1", () => {
    const missing = plan({}, nothing);
    const reason = missing.missing ?? "";
    expect(decide(missing, {})).toEqual({ kind: "skip", reason });
    expect(decide(missing, { RAVEL_E2E_REQUIRE: "1" })).toEqual({ kind: "fail", reason });
    expect(decide(missing, { RAVEL_E2E_REQUIRE: "0" })).toEqual({ kind: "skip", reason });
    expect(decide(plan({}, everything), { RAVEL_E2E_REQUIRE: "1" })).toEqual({ kind: "run" });
  });
});
