import { after, type TestContext } from 'node:test';
import uWS from 'uWebSockets.js';
import { DEFAULT_LIMITS, type Limits } from '../src/protocol/limits.ts';
import type { StorageAdapter } from '../src/storage/adapter.ts';
import { MemoryStorage } from '../src/storage/memory.ts';
import { PostgresStorage } from '../src/storage/postgres.ts';
import { PG_URL, dropSchema, isPostgres, uniqueSchema } from '../harness/pg.ts';
import { assertNoLeftovers, RedisProcess } from '../harness/redis.ts';
import { connectRedis, type Redis } from '../src/fanout/redis.ts';
import { RtdbClient, type ClientOptions } from '../harness/client.ts';
import { signDevToken } from '../src/gateway/auth.ts';
import { startGateway, type Gateway, type GatewayOptions } from '../src/gateway/server.ts';

export const wsUrl = (port: number): string => `ws://127.0.0.1:${port}`;

/** A token the default DevHs256Validator accepts. */
export const goodToken = (userId = 'u_test'): string =>
  signDevToken({ sub: userId, exp: Math.floor(Date.now() / 1000) + 3600 });

/** Open a bare WebSocket and resolve once it is open. */
export const openRaw = (port: number): Promise<WebSocket> =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl(port));
    ws.addEventListener('open', () => resolve(ws), { once: true });
    ws.addEventListener('error', () => reject(new Error('socket error')), { once: true });
  });

/** Next text message, JSON-parsed. */
export const nextFrame = <T = Record<string, unknown>>(ws: WebSocket): Promise<T> =>
  new Promise((resolve) =>
    ws.addEventListener('message', (ev) => resolve(JSON.parse(String(ev.data)) as T), { once: true }),
  );

export const nextClose = (ws: WebSocket): Promise<{ code: number; reason: string }> =>
  new Promise((resolve) =>
    ws.addEventListener('close', (ev) => resolve({ code: ev.code, reason: ev.reason }), { once: true }),
  );

export { sleep, waitUntil } from '../harness/scenario.ts';

/**
 * A server that completes hello but never answers ping — the only way to exercise the client's
 * §5 pong timeout over a real socket, since the real gateway always pongs.
 */
export async function startDeafServer(): Promise<{ port: number; close(): void }> {
  const app = uWS.App().ws<Record<string, never>>('/*', {
    idleTimeout: 0,
    sendPingsAutomatically: false,
    message(ws, message) {
      const frame = JSON.parse(Buffer.from(message).toString('utf8')) as { type?: string };
      if (frame.type === 'hello') {
        ws.send(JSON.stringify({ type: 'helloAck', rev: 0, epoch: 1, region: 'test', session: 's_deaf' }), false);
      }
      // pings are deliberately dropped on the floor
    },
  });
  const token = await new Promise<uWS.us_listen_socket>((resolve, reject) => {
    app.listen(0, (t) => (t ? resolve(t) : reject(new Error('deaf server failed to listen'))));
  });
  return { port: uWS.us_socket_local_port(token), close: () => uWS.us_listen_socket_close(token) };
}

/** Resolve on the first `frame` event matching `pred`. */
export function waitForFrame<T extends Record<string, unknown>>(
  emitter: import('node:events').EventEmitter,
  pred: (f: T) => boolean,
): Promise<T> {
  return new Promise((resolve) => {
    const on = (f: T): void => {
      if (!pred(f)) return;
      emitter.off('frame', on);
      resolve(f);
    };
    emitter.on('frame', on);
  });
}

/**
 * The store one test gateway runs on. Under `RTDB_STORAGE=postgres` every harness gets its OWN
 * schema in the database `RTDB_PG_URL` names — same "one database, many independent stores"
 * mechanism as the storage tests, so revs still start at 1 for each test. Otherwise: memory, and
 * nothing about the existing suites changes.
 */
export function testStorage(limits: Limits): { storage: StorageAdapter; cleanup: () => Promise<void> } {
  if (!isPostgres()) return { storage: new MemoryStorage(limits), cleanup: () => Promise.resolve() };
  const schema = uniqueSchema();
  const storage = new PostgresStorage({ url: PG_URL, limits, schema });
  return {
    storage,
    cleanup: async () => {
      await storage.close();
      await dropSchema(schema);
    },
  };
}

/**
 * `RTDB_TEST_REDIS=1` runs the SAME suites over §8's bus instead of the in-process stream: deltas
 * travel oplog -> leader -> Redis Stream -> consumer -> connection. Unset, not one line of this file
 * does anything (WORKLOAD §0.7).
 */
export const redisMode = (): boolean => process.env['RTDB_TEST_REDIS'] === '1';

/** One redis-server per test FILE, one bus keyspace per harness — the pg "one database, many
 * schemas" pattern, so each gateway's shard is independent and revs still start at 1. */
let redisProc: Promise<RedisProcess> | null = null;
let shardSeq = 0;

after(async () => {
  if (!redisProc) return;
  await (await redisProc).stop();
  assertNoLeftovers();
});

async function testBus(t: TestContext): Promise<{ redis: Redis; shard: string }> {
  redisProc ??= RedisProcess.start();
  const redis = await connectRedis((await redisProc).url);
  t.after(() => {
    try {
      redis.destroy();
    } catch {
      /* already gone */
    }
  });
  return { redis, shard: `s${++shardSeq}` };
}

/** A gateway plus a factory for connected clients, all torn down with the test. */
export async function harness(
  t: TestContext,
  opts: GatewayOptions = {},
): Promise<{ gw: Gateway; connect: (o?: Partial<ClientOptions>) => Promise<RtdbClient> }> {
  const owned = opts.storage ? null : testStorage(opts.limits ?? DEFAULT_LIMITS);
  const bus = redisMode() && !opts.redis ? await testBus(t) : null;
  const gw = await startGateway({ ...opts, ...(owned ? { storage: owned.storage } : {}), ...(bus ?? {}) });
  // ONE hook, so the order is explicit: the gateway (and its dispatcher) stops before the store it
  // was reading closes underneath it.
  t.after(async () => {
    gw.close();
    await owned?.cleanup();
  });
  return {
    gw,
    connect: async (o = {}) => {
      const client = new RtdbClient({
        url: wsUrl(gw.port),
        token: goodToken(),
        pingIntervalMs: 60_000, // pings are Gate B's business; keep them out of these tests
        ...(opts.limits ? { limits: opts.limits } : {}),
        ...o,
      });
      t.after(() => client.close());
      client.connect();
      await client.ready();
      return client;
    },
  };
}


/** A raw socket that has completed hello — for tests that need to drive the wire directly. */
export async function rawConnected(port: number, token = goodToken()): Promise<WebSocket> {
  const ws = await openRaw(port);
  const ack = nextFrame(ws);
  ws.send(JSON.stringify({ type: 'hello', proto: 1, token }));
  await ack;
  return ws;
}

/** Collect frames off a raw socket, unwrapping batches the way a client would (§3). */
export function collect(ws: WebSocket): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  ws.addEventListener('message', (ev) => {
    const f = JSON.parse(String(ev.data)) as Record<string, unknown>;
    if (f['type'] === 'batch') out.push(...(f['frames'] as Record<string, unknown>[]));
    else out.push(f);
  });
  return out;
}
