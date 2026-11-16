/**
 * Starts a built ravel-server with `--agents` for the e2e tests, against
 * MemoryStore (the default) or the compose RustFS store
 * (`RAVEL_E2E_STORE=rustfs`), and says what is missing when it cannot.
 */
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** The repository root: this file is `sdk/typescript/packages/agents/e2e/server.ts`. */
export const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../..");
export const COMPOSE_FILE = join(REPO_ROOT, "deploy/docker-compose/rustfs.yml");
const READY_TIMEOUT_MS = 60_000;

export const TOKENS = {
  client: "e2e-client",
  worker: "e2e-worker",
  admin: "e2e-admin",
} as const;

export type Store = "memory" | "rustfs";

export interface Plan {
  readonly binary: string;
  readonly store: Store;
  /** Why the e2e suite cannot run here, or undefined when it can. */
  readonly missing: string | undefined;
}

export type Env = Readonly<Record<string, string | undefined>>;

export interface Probes {
  readonly exists: (file: string) => boolean;
  readonly dockerCompose: () => boolean;
}

function dockerComposeAvailable(): boolean {
  const probe = spawnSync("docker", ["compose", "version"], { stdio: "ignore" });
  return probe.error === undefined && probe.status === 0;
}

const HOST_PROBES: Probes = { exists: existsSync, dockerCompose: dockerComposeAvailable };

/** `RAVEL_SERVER_BIN`, else `$CARGO_TARGET_DIR/debug/ravel-server`, else `<repo>/target/debug/ravel-server`. */
export function serverBinary(env: Env = process.env): string {
  const explicit = env.RAVEL_SERVER_BIN;
  if (explicit !== undefined && explicit !== "") return explicit;
  const target = env.CARGO_TARGET_DIR;
  return join(target !== undefined && target !== "" ? target : join(REPO_ROOT, "target"), "debug", "ravel-server");
}

/** What the suite would run against, decided without starting anything. */
export function plan(env: Env = process.env, probes: Probes = HOST_PROBES): Plan {
  const binary = serverBinary(env);
  const requested = env.RAVEL_E2E_STORE ?? "memory";
  if (requested !== "memory" && requested !== "rustfs") {
    return { binary, store: "memory", missing: `RAVEL_E2E_STORE=${requested} is neither memory nor rustfs` };
  }
  const store: Store = requested;
  if (!probes.exists(binary)) {
    return {
      binary,
      store,
      missing:
        `ravel-server binary not found at ${binary}; build it with ` +
        "`cargo build --locked -p ravel-server --features agents` or set RAVEL_SERVER_BIN",
    };
  }
  if (store === "rustfs" && !probes.dockerCompose()) {
    return {
      binary,
      store,
      missing: "RAVEL_E2E_STORE=rustfs needs `docker compose`, which is not installed or not running",
    };
  }
  return { binary, store, missing: undefined };
}

/**
 * Run, skip, or fail. A missing prerequisite skips the suite unless
 * `RAVEL_E2E_REQUIRE=1`, which turns the skip into a failure with the same
 * reason, so a run that must exercise the server cannot pass with nothing run.
 */
export type Decision =
  | { readonly kind: "run" }
  | { readonly kind: "skip"; readonly reason: string }
  | { readonly kind: "fail"; readonly reason: string };

export function decide(target: Plan, env: Env = process.env): Decision {
  if (target.missing === undefined) return { kind: "run" };
  return env.RAVEL_E2E_REQUIRE === "1"
    ? { kind: "fail", reason: target.missing }
    : { kind: "skip", reason: target.missing };
}

function freePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      probe.close(() => {
        resolvePort(port);
      });
    });
  });
}

function compose(...args: string[]): number | null {
  return spawnSync("docker", ["compose", "-f", COMPOSE_FILE, ...args], { stdio: "inherit" }).status;
}

function composeOrThrow(...args: string[]): void {
  const code = compose(...args);
  if (code !== 0) throw new Error(`docker compose ${args.join(" ")} exited ${String(code)}`);
}

function rustfsRunning(): boolean {
  const ps = spawnSync("docker", ["compose", "-f", COMPOSE_FILE, "ps", "--quiet", "--status", "running", "rustfs"], {
    encoding: "utf8",
  });
  return ps.status === 0 && ps.stdout.trim() !== "";
}

/** Starts RustFS unless it already runs; answers whether this call started it. */
function startRustfs(): boolean {
  const already = rustfsRunning();
  if (!already) composeOrThrow("up", "-d", "rustfs");
  try {
    composeOrThrow("run", "--rm", "createbucket");
  } catch (error) {
    if (!already) compose("stop", "rustfs");
    throw error;
  }
  return !already;
}

function storeArgs(store: Store): string[] {
  if (store === "memory") return ["--store", "memory"];
  return [
    "--store",
    "s3",
    "--s3-endpoint",
    process.env.RAVEL_E2E_S3_ENDPOINT ?? "http://127.0.0.1:9000",
    "--s3-allow-http",
    "--s3-bucket",
    "ravel-dev",
    "--s3-region",
    "us-east-1",
    // The compose file's published development pair, not a credential.
    "--s3-access-key",
    "ravel",
    "--s3-secret-key",
    "ravel-dev-secret",
  ];
}

export interface RunningServer {
  readonly url: string;
  readonly store: Store;
  /** The server's output so far, for a failure message. */
  output(): string;
  /** Stops the server, and the RustFS container when this run started it. */
  stop(): Promise<void>;
}

export async function startServer(binary: string, store: Store): Promise<RunningServer> {
  const http = await freePort();
  const grpc = await freePort();
  const startedRustfs = store === "rustfs" && startRustfs();
  const args = [
    ...storeArgs(store),
    "--agents",
    "--tenant-hash-unkeyed",
    "--listen-http",
    `127.0.0.1:${String(http)}`,
    "--listen-grpc",
    `127.0.0.1:${String(grpc)}`,
    "--tenant-token",
    `${TOKENS.client}=acme;agents`,
    "--tenant-token",
    `${TOKENS.worker}=acme;agents_worker`,
    "--tenant-token",
    `${TOKENS.admin}=acme;agents_admin`,
  ];
  const lines: string[] = [];
  const child: ChildProcess = spawn(binary, args, { stdio: ["ignore", "pipe", "pipe"] });
  const keep = (chunk: Buffer) => {
    lines.push(chunk.toString("utf8"));
  };
  child.stdout?.on("data", keep);
  child.stderr?.on("data", keep);
  const exited = new Promise<void>((done) => child.once("exit", () => {
    done();
  }));
  const url = `http://127.0.0.1:${String(http)}`;
  const server: RunningServer = {
    url,
    store,
    output: () => lines.join(""),
    stop: async () => {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGTERM");
        const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
        await exited;
        clearTimeout(timer);
      }
      if (startedRustfs) compose("stop", "rustfs");
    },
  };

  const deadline = Date.now() + READY_TIMEOUT_MS;
  for (;;) {
    if (child.exitCode !== null || child.signalCode !== null) {
      const how = child.signalCode === null ? `exited ${String(child.exitCode)}` : `was killed by ${child.signalCode}`;
      await server.stop();
      throw new Error(`ravel-server ${how} before ready:\n${server.output()}`);
    }
    const ready = await fetch(`${url}/readyz`).then((r) => r.ok, () => false);
    if (ready) return server;
    if (Date.now() > deadline) {
      await server.stop();
      throw new Error(`ravel-server not ready within ${String(READY_TIMEOUT_MS)} ms:\n${server.output()}`);
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}
