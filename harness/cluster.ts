import { RtdbClient, type ClientOptions } from './client.ts';
import { dropSchema, PG_URL, uniqueSchema } from './pg.ts';
import { RedisProcess } from './redis.ts';
import { GatewayProcess } from './scenario.ts';
import { busKeys, connectRedis, streamTailRev, type Redis } from '../src/fanout/redis.ts';
import { makeLimits, type Limits } from '../src/protocol/limits.ts';

/**
 * Two gateway PROCESSES over one shard: one Postgres schema, one Redis, exactly the deployment §8
 * describes. Two processes cannot share an in-memory store, so the shard has to be a real database.
 *
 * `a` is started first and `startGateway` does not resolve until its election is decided, so `a` is
 * deterministically the leader and `b` the follower — no polling to find out which is which.
 */
export interface Cluster {
  redis: RedisProcess;
  a: GatewayProcess;
  b: GatewayProcess;
  limits: Limits;
  /** The rev the shard's stream carries — proof that a PUBLISHER is alive, not just a consumer. */
  tail(): Promise<number | null>;
  /** `token` is required: a harness that defaults it would make an auth bug look like a pass. */
  connect(url: string, o: Partial<ClientOptions> & { token: string }): Promise<RtdbClient>;
  stop(): Promise<void>;
}

export async function startCluster(overrides: Partial<Limits> = {}): Promise<Cluster> {
  const limits = makeLimits({ BACKOFF_CAP_MS: 40, ...overrides });
  const redis = await RedisProcess.start();
  const schema = uniqueSchema('cluster');
  const env = {
    RTDB_STORAGE: 'postgres',
    RTDB_PG_URL: PG_URL,
    RTDB_PG_SCHEMA: schema,
    RTDB_REDIS_URL: redis.url,
  };
  const a = await GatewayProcess.start(overrides, 0, undefined, env);
  const b = await GatewayProcess.start(overrides, 0, undefined, env);

  let probe: Redis | null = null;
  const clients: RtdbClient[] = [];
  return {
    redis,
    a,
    b,
    limits,
    async tail() {
      probe ??= await connectRedis(redis.url);
      return streamTailRev(probe, busKeys().stream);
    },
    async connect(url, o) {
      const client = new RtdbClient({ url, limits, pingIntervalMs: 60_000, ...o });
      clients.push(client);
      client.connect();
      await client.ready();
      return client;
    },
    async stop() {
      for (const c of clients) c.close();
      await a.stop();
      await b.stop();
      try {
        probe?.destroy();
      } catch {
        /* already gone */
      }
      await redis.stop();
      await dropSchema(schema).catch(() => undefined);
    },
  };
}
