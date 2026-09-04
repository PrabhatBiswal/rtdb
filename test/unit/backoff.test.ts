import assert from 'node:assert/strict';
import test from 'node:test';
import { backoffDelay } from '../../harness/client.ts';
import { DEFAULT_LIMITS, makeLimits } from '../../src/protocol/limits.ts';

test('§6 full jitter: delay is uniform in [0, min(30s, 1s*2^attempt))', () => {
  // random() = 0 is the floor, random() -> 1 approaches the ceiling.
  const floor = (a: number) => backoffDelay(a, DEFAULT_LIMITS, () => 0);
  const ceil = (a: number) => backoffDelay(a, DEFAULT_LIMITS, () => 0.999999);
  for (let a = 0; a < 10; a++) assert.equal(floor(a), 0);
  assert.equal(ceil(0), 999);
  assert.equal(ceil(1), 1999);
  assert.equal(ceil(2), 3999);
  assert.equal(ceil(4), 15999);
});

test('§6 backoff is capped at 30s however many attempts have failed', () => {
  const at = (a: number) => backoffDelay(a, DEFAULT_LIMITS, () => 0.999999);
  assert.equal(at(5), 29999); // 32s would exceed the cap
  assert.equal(at(20), 29999);
  assert.equal(at(100), 29999); // 2^100 must not overflow into something un-capped
});

test('the cap is configurable for tests', () => {
  const fast = makeLimits({ BACKOFF_CAP_MS: 50 });
  assert.equal(backoffDelay(10, fast, () => 0.999999), 49);
});

test('real randomness stays inside the window', () => {
  for (let i = 0; i < 500; i++) {
    const d = backoffDelay(3, DEFAULT_LIMITS);
    assert.ok(d >= 0 && d < 8000, `delay ${d} outside [0, 8000)`);
  }
});
