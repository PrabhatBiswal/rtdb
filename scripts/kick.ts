/**
 * §10 admin plane: publish one kick onto a shard's out-of-band channel. Every gateway on that shard
 * closes the user's connections with 4403; combined with §3's subscribe-time auth, kick + the
 * client's own reconnect IS the revocation point.
 *
 *   node --import tsx scripts/kick.ts --url redis://127.0.0.1:6379 --user u_123 [--reason ban] [--shard 0]
 *
 * Exits non-zero if nothing was listening — a kick nobody received is not a revocation.
 */
import type { Kick } from '../src/protocol/frames.ts';
import { busKeys, connectRedis } from '../src/fanout/redis.ts';

const flag = (name: string): string | undefined => {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? undefined : process.argv[i + 1];
};

const url = flag('url') ?? process.env['RTDB_REDIS_URL'];
const userId = flag('user');
if (!url || !userId) {
  console.error('usage: kick.ts --url <redis-url> --user <userId> [--reason <text>] [--shard <n>]');
  process.exit(2);
}

const reason = flag('reason');
const frame: Kick = { type: 'kick', target: { userId }, ...(reason ? { reason } : {}) };

const redis = await connectRedis(url);
const receivers = await redis.publish(busKeys(flag('shard') ?? 0).kick, JSON.stringify(frame));
await redis.close();

console.log(`kicked ${userId} on ${receivers} gateway(s)`);
if (receivers === 0) process.exit(1);
