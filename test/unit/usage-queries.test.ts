/**
 * §5.25 Gate 3: the Load expression must survive MORE THAN ONE GATEWAY. `rtdb_lock_hold_ms` is
 * scraped from every gateway, so `on() group_left()` against it finds N series on the right and
 * fails — on production, with a real 502 behind it. This pins the shape without needing Prometheus:
 * the query text may not join against an unaggregated `rtdb_lock_hold_ms`.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const SERVER = readFileSync(fileURLToPath(new URL('../../console/auth-server.mjs', import.meta.url)), 'utf8');
const DASH = readFileSync(fileURLToPath(new URL('../../deploy/grafana/dashboards/rtdb.json', import.meta.url)), 'utf8');

test('no Load query joins against a per-gateway rtdb_lock_hold_ms (§5.25 Gate 3)', () => {
  for (const [name, text] of [['auth-server', SERVER], ['grafana dashboard', DASH]] as const) {
    const uses = [...text.matchAll(/rtdb_lock_hold_ms/g)];
    assert.ok(uses.length > 0, `${name} still computes Load`);
    // The failure mode, by name: a vector match against the raw metric. Collapsed with an
    // aggregator it is one series whatever the fleet size, which is the only version that works
    // on more than one gateway.
    assert.doesNotMatch(
      text,
      /group_left\(\)\s*rtdb_lock_hold_ms/,
      `${name}: on()/group_left() against the raw metric fails with "duplicate series for the match group" on a fleet of 2+`,
    );
    // Every use INSIDE A QUERY must be aggregated. Prose and the metric's own declaration mention
    // the name too, so the check is on the arithmetic: a `*` or `/` next to a bare metric name.
    assert.doesNotMatch(
      text,
      /[*/]\s*(on\(\)\s*)?(group_left\(\)\s*)?rtdb_lock_hold_ms/,
      `${name}: Load must multiply by an AGGREGATED hold — scalar(max(rtdb_lock_hold_ms)) — not the raw metric`,
    );
    assert.match(
      text,
      /scalar\(\s*max\(\s*rtdb_lock_hold_ms\s*\)\s*\)/,
      `${name}: and it must use the collapsed form`,
    );
  }
});
