import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import type { Json } from '../src/protocol/frames.ts';
import type { Limits } from '../src/protocol/limits.ts';
import { devSecret } from '../src/gateway/auth.ts';
import { RtdbClient } from './client.ts';
import { dropSchema, isPostgres, PG_URL, uniqueSchema } from './pg.ts';

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export async function waitUntil(
  pred: () => boolean | Promise<boolean>,
  label = 'condition',
  ms = 5000,
): Promise<void> {
  const deadline = Date.now() + ms;
  while (!(await pred())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await sleep(2);
  }
}

const MAIN = fileURLToPath(new URL('../src/gateway/main.ts', import.meta.url));

/** The gateway as a real, killable OS process (WORKLOAD §5). */
export class GatewayProcess {
  #child: ChildProcess | null = null;
  /** Assigned from the child's first stdout line, then fixed — a restart reuses it, so clients
   * reconnect to the URL they already hold. */
  port = 0;

  /** Postgres schemas this process has run on, so they can be dropped when it stops for good. */
  readonly #schemas: string[] = [];
  #schema: string | null = null;

  private constructor(
    private readonly limits: Partial<Limits>,
    private readonly persist?: string,
    /** Extra environment for the child — `RTDB_REDIS_URL` and friends (WORKLOAD §2). */
    private readonly extraEnv: NodeJS.ProcessEnv = {},
  ) {}

  static async start(
    limits: Partial<Limits> = {},
    port = 0,
    persist?: string,
    extraEnv: NodeJS.ProcessEnv = {},
  ): Promise<GatewayProcess> {
    const gw = new GatewayProcess(limits, persist, extraEnv);
    gw.#child = await gw.#spawn(port);
    return gw;
  }

  get url(): string {
    return `ws://127.0.0.1:${this.port}`;
  }

  /** The Postgres schema this gateway's shard lives in, or null on the memory backend. */
  get schema(): string | null {
    return this.#schema;
  }

  async #spawn(port: number): Promise<ChildProcess> {
    const child = spawn(process.execPath, ['--import', 'tsx', MAIN], {
      env: {
        ...process.env,
        RTDB_PORT: String(port),
        RTDB_LIMITS: JSON.stringify(this.limits),
        // main.ts REFUSES to boot on postgres without this (a real deployment must not verify
        // tokens with the public default), and every spawn here is a real main.ts against a real
        // database. Not a bypass of that guard: `devSecret()` resolves to the SAME value this
        // process signs tokens with, so setting only the child's would make the child reject every
        // token the parent mints. Whatever the suite runs under, both halves agree.
        RTDB_DEV_SECRET: devSecret(),
        ...this.#storageEnv(),
        ...this.extraEnv,
      },
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    const line: string = await new Promise((resolve, reject) => {
      child.stdout?.setEncoding('utf8');
      child.stdout?.once('data', resolve);
      child.once('exit', (code) => reject(new Error(`gateway exited before listening (code ${code})`)));
    });
    // U3 gave the gateway a stdout it actually uses. This pipe has exactly one reader — the line
    // above — and an unread pipe fills at ~64 KiB and then BLOCKS the writer, which here means the
    // gateway wedges mid-scenario. Same lesson, same fix as redis.ts: read the one line, then drain.
    child.stdout?.resume();
    const listening = /rtdb listening (\d+)/.exec(line);
    if (!listening) throw new Error(`unexpected gateway output: ${line}`);
    this.port = Number(listening[1]);
    return child;
  }

  /**
   * Which store the child comes up on. Under Postgres the DATABASE is the durability, so `persist`
   * stops meaning "a file" and starts meaning what it always meant to the scenario: does this
   * gateway keep its past across a restart? Asked to persist, it keeps its schema (S10: same epoch,
   * data intact). Asked not to, every spawn mints a new one, so the shard comes back with no past —
   * exactly the reset §2's epoch exists to announce (S11).
   */
  #storageEnv(): NodeJS.ProcessEnv {
    if (!isPostgres()) return this.persist ? { RTDB_PERSIST: this.persist } : {};
    if (this.#schema === null || !this.persist) {
      this.#schema = uniqueSchema('chaos');
      this.#schemas.push(this.#schema);
    }
    // main.ts refuses to guess a URL — a production gateway silently defaulting to localhost is a
    // worse failure than not starting — so the harness, which knows it, hands it over.
    return { RTDB_PG_URL: PG_URL, RTDB_PG_SCHEMA: this.#schema };
  }

  /** SIGKILL by default: no close frames, no graceful shutdown — the process simply stops. */
  async kill(signal: NodeJS.Signals = 'SIGKILL'): Promise<void> {
    const child = this.#child;
    if (!child) return;
    this.#child = null;
    const exited = once(child, 'exit');
    child.kill(signal);
    await exited;
  }

  async restart(): Promise<void> {
    await this.kill();
    this.#child = await this.#spawn(this.port);
  }

  async stop(): Promise<void> {
    await this.kill('SIGTERM');
    // Only schemas this object minted, and only once the process holding them is gone.
    for (const schema of this.#schemas.splice(0)) await dropSchema(schema).catch(() => undefined);
  }
}

/**
 * Read the server's own view of a path, over the wire — the only honest reference when the gateway
 * is a separate process. Uses a throwaway connection so it cannot disturb the clients under test.
 */
export async function serverValue(url: string, token: string, path: string): Promise<Json> {
  const probe = new RtdbClient({ url, token, autoReconnect: false, pingIntervalMs: 60_000 });
  probe.connect();
  await probe.ready();
  try {
    let seen = false;
    probe.listen(path, () => (seen = true));
    await waitUntil(() => seen, `snapshot of "${path}"`);
    return probe.value(path);
  } finally {
    probe.close();
  }
}

/**
 * Gate D's standing assertion: every client mirror equals server state on every path it subscribes
 * to, with nothing left pending. Compares serverState (not the view) — a converged client has an
 * empty overlay, and asserting that separately is what catches a write that never settled.
 */
export async function assertConverged(clients: RtdbClient[], url: string, token: string): Promise<void> {
  for (const [i, client] of clients.entries()) {
    assert.deepEqual(client.pendingWriteIds, [], `client ${i} still has unacked writes`);
    for (const sub of client.subscriptions) {
      const expected = await serverValue(url, token, sub.path);
      assert.deepEqual(
        client.mirror.serverValue(sub.path),
        expected,
        `client ${i} diverged from the server at "${sub.path}"`,
      );
      assert.deepEqual(client.value(sub.path), expected, `client ${i} view != serverState at "${sub.path}"`);
    }
  }
}
