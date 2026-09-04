import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import test, { after } from 'node:test';
import { assertNoLeftovers, RedisProcess } from '../../harness/redis.ts';
import { connectRedis } from '../../src/fanout/redis.ts';

/** WORKLOAD §0.8 / §5: a clean run leaves no redis-server alive and no temp dir behind. */
after(() => assertNoLeftovers());

test('spawned instances are independent, and stopping one removes every trace of it', async () => {
  const a = await RedisProcess.start();
  const b = await RedisProcess.start();
  assert.notEqual(a.port, b.port);
  assert.ok(existsSync(a.dir) && existsSync(b.dir));

  const ca = await connectRedis(a.url);
  const cb = await connectRedis(b.url);
  await ca.set('k', 'a');
  assert.equal(await ca.get('k'), 'a');
  assert.equal(await cb.get('k'), null, 'two spawned servers must not share state');
  await ca.destroy();
  await cb.destroy();

  const pid = a.pid as number;
  await a.stop();
  assert.throws(() => process.kill(pid, 0), /ESRCH/, 'the server we spawned must be gone');
  assert.equal(existsSync(a.dir), false, 'its data directory must be gone too');
  await b.stop();
});

test('a SIGKILLed instance comes back on the same port, and comes back empty', async () => {
  const r = await RedisProcess.start();
  const url = r.url;
  const before = await connectRedis(url);
  await before.set('k', 'v');
  await before.destroy();

  await r.restart(); // `--save ''`: nothing was persisted, so this is the honest post-crash state
  assert.equal(r.url, url, 'the port must survive, or every client holds a dead URL');
  const after = await connectRedis(url);
  assert.equal(await after.get('k'), null);
  await after.destroy();
  await r.stop();
});

test('assertNoLeftovers fails while a spawned server is still up', async () => {
  const r = await RedisProcess.start();
  assert.throws(() => assertNoLeftovers(), /still alive/);
  await r.stop();
  assertNoLeftovers();
});
