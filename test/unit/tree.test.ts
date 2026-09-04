import assert from 'node:assert/strict';
import test from 'node:test';
import { DEFAULT_LIMITS, makeLimits } from '../../src/protocol/limits.ts';
import {
  ancestorsInclusive,
  isAncestorOrEqual,
  isRelevant,
  relativePath,
} from '../../src/protocol/path.ts';
import { flatten, type Leaf, unflatten } from '../../src/storage/tree.ts';

const leaves = (path: string, value: unknown, limits = DEFAULT_LIMITS): Leaf[] => {
  const r = flatten(path, value as never, limits);
  assert.equal(r.ok, true, `flatten failed: ${JSON.stringify(r)}`);
  return (r as { leaves: Leaf[] }).leaves;
};

test('prefix predicates do not confuse sibling names that share a prefix', () => {
  assert.equal(isAncestorOrEqual('MPK_1', 'MPK_10'), false, '"MPK_1" is not an ancestor of "MPK_10"');
  assert.equal(isAncestorOrEqual('MPK_1', 'MPK_1/x'), true);
  assert.equal(isAncestorOrEqual('a', 'a'), true);
  assert.equal(isAncestorOrEqual('', 'anything/at/all'), true, 'root is everyone’s ancestor');
  assert.equal(isAncestorOrEqual('a/b', 'a'), false);
});

test('§3 relevance is symmetric: at-or-under, in either direction', () => {
  assert.equal(isRelevant('a/b', 'a'), true);
  assert.equal(isRelevant('a', 'a/b'), true);
  assert.equal(isRelevant('a/b', 'a/c'), false);
  assert.equal(isRelevant('', 'a/b'), true);
});

test('ancestorsInclusive walks root-first and includes the path itself', () => {
  assert.deepEqual(ancestorsInclusive('a/b/c'), ['', 'a', 'a/b', 'a/b/c']);
  assert.deepEqual(ancestorsInclusive(''), ['']);
  assert.equal(ancestorsInclusive('a'.repeat(1) + '/b'.repeat(1)).length, 3);
});

test('relativePath strips the ancestor, and is empty when they are equal', () => {
  assert.equal(relativePath('a', 'a/b/c'), 'b/c');
  assert.equal(relativePath('a/b', 'a/b'), '');
  assert.equal(relativePath('', 'a/b'), 'a/b');
});

test('flatten walks objects and stops at scalars and arrays (§1)', () => {
  assert.deepEqual(leaves('p', { name: 'Ravi', stats: { wins: 3 } }), [
    { path: 'p/name', value: 'Ravi' },
    { path: 'p/stats/wins', value: 3 },
  ]);
  assert.deepEqual(leaves('p', 42), [{ path: 'p', value: 42 }]);
  assert.deepEqual(leaves('p', [1, 2]), [{ path: 'p', value: [1, 2] }], 'arrays are opaque');
  assert.deepEqual(leaves('', { a: 1 }), [{ path: 'a', value: 1 }], 'root writes work');
});

test('null and empty objects produce no leaves — both mean "nothing stored here" (§1)', () => {
  assert.deepEqual(leaves('p', null), []);
  assert.deepEqual(leaves('p', {}), []);
  assert.deepEqual(leaves('p', { a: null, b: {}, c: 1 }), [{ path: 'p/c', value: 1 }]);
});

test('value keys are validated as path segments, not silently nested', () => {
  for (const bad of [{ 'a/b': 1 }, { 'a.b': 1 }, { '': 1 }, { 'a#b': 1 }]) {
    const r = flatten('p', bad as never, DEFAULT_LIMITS);
    assert.equal(r.ok, false, `expected ${JSON.stringify(bad)} to be rejected`);
  }
});

test('flatten stops at MAX_LEAVES_PER_WRITE and says TOOBIG (§9)', () => {
  const small = makeLimits({ MAX_LEAVES_PER_WRITE: 3 });
  assert.equal(flatten('p', { a: 1, b: 2, c: 3 } as never, small).ok, true);
  const r = flatten('p', { a: 1, b: 2, c: 3, d: 4 } as never, small);
  assert.deepEqual({ ok: r.ok, tooBig: (r as { tooBig?: true }).tooBig }, { ok: false, tooBig: true });
});

test('unflatten is flatten’s inverse, including scalars sitting at the root', () => {
  const value = { name: 'Ravi', stats: { wins: 3, losses: 1 }, list: [1, 2] };
  assert.deepEqual(unflatten('p', leaves('p', value)), value);
  assert.deepEqual(unflatten('p', leaves('p', 42)), 42);
  assert.equal(unflatten('p', []), null, 'no leaves means null');
  assert.deepEqual(unflatten('', leaves('', { a: { b: 1 } })), { a: { b: 1 } });
});
