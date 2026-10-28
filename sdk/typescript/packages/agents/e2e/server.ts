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

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
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

function serverBinary(): string {
  const explicit = process.env.RAVEL_SERVER_BIN;
  if (explicit !== undefined && explicit !== "") return explicit;
  const target = process.env.CARGO_TARGET_DIR ?? join(REPO_ROOT, "target");
  return join(target, "debug", "ravel-server");
}

function dockerComposeAvailable(): boolean {
  const probe = spawnSync("docker", ["compose", "version"], { stdio: "ignore" });
  return probe.error === undefined && probe.status === 0;
}

/** What the suite would run against, decided without starting anything. */
export function plan(): Plan {
  const binary = serverBinary();
  const requested = process.env.RAVEL_E2E_STORE ?? "memory";
  if (requested !== "memory" && requested !== "rustfs") {
    return { binary, store: "memory", missing: `RAVEL_E2E_STORE=${requested} is neither memory nor rustfs` };
  }
  const store: Store = requested;
  if (!existsSync(binary)) {
    return {
      binary,
      store,
      missing:
        `ravel-server binary not found at ${binary}; build it with ` +
        "`cargo build --locked -p ravel-server --features agents` or set RAVEL_SERVER_BIN",
    };
  }
  if (store === "rustfs" && !dockerComposeAvailable()) {
    return {
      binary,
      store,
      missing: "RAVEL_E2E_STORE=rustfs needs `docker compose`, which is not installed or not running",
    };
  }
  return { binary, store, missing: undefined };
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

function compose(...args: string[]): void {
  const file = join(REPO_ROOT, "deploy/docker-compose/rustfs.yml");
  const run = spawnSync("docker", ["compose", "-f", file, ...args], { stdio: "inherit" });
  if (run.status !== 0) throw new Error(`docker compose ${args.join(" ")} exited ${String(run.status)}`);
}

function storeArgs(store: Store): string[] {
  if (store === "memory") return ["--store", "memory"];
  compose("up", "-d", "rustfs");
  compose("run", "--rm", "createbucket");
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
  stop(): Promise<void>;
}

export async function startServer(binary: string, store: Store): Promise<RunningServer> {
  const http = await freePort();
  const grpc = await freePort();
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
      if (child.exitCode !== null || child.signalCode !== null) return;
      child.kill("SIGTERM");
      const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
      await exited;
      clearTimeout(timer);
    },
  };

  const deadline = Date.now() + READY_TIMEOUT_MS;
  for (;;) {
    if (child.exitCode !== null) {
      throw new Error(`ravel-server exited ${String(child.exitCode)} before ready:\n${server.output()}`);
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
