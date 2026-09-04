import assert from 'node:assert/strict';
import test from 'node:test';
import { DEFAULT_LIMITS, makeLimits } from '../../src/protocol/limits.ts';
import { joinPath, segments, validatePath } from '../../src/protocol/path.ts';

const ok = (p: unknown, limits = DEFAULT_LIMITS) =>
  assert.equal(validatePath(p, limits), null, `expected ${JSON.stringify(p)} to be valid`);
const bad = (p: unknown, limits = DEFAULT_LIMITS) =>
  assert.notEqual(validatePath(p, limits), null, `expected ${JSON.stringify(p)} to be invalid`);

test('root and ordinary paths are valid', () => {
  ok('');
  ok('a');
  ok('MPK_1010/1474396/score');
  ok('a-b_c~!@%^&*()+=');
});

test('slash placement rules (§1: no leading/trailing/empty segments)', () => {
  bad('/a');
  bad('a/');
  bad('a//b');
  bad('/');
});

test('forbidden segment characters: / . # $ [ ]', () => {
  for (const p of ['a.b', 'a#b', 'a$b', 'a[b', 'a]b', 'x/y.z']) bad(p);
});

test('control characters are forbidden in segments (§1, v1.1)', () => {
  for (const c of ['\u0000', '\u0001', '\u0009', '\u000a', '\u001f', '\u007f']) {
    bad(`a${c}b`);
    bad(`ok/a${c}`);
  }
  ok('a\u0080b'); // U+0080 is not in the forbidden range
});

test('non-strings are invalid', () => {
  for (const p of [undefined, null, 42, {}, ['a'], true]) bad(p);
});

test('depth limit is 32 segments', () => {
  ok(Array.from({ length: 32 }, (_, i) => `s${i}`).join('/'));
  bad(Array.from({ length: 33 }, (_, i) => `s${i}`).join('/'));
});

test('length limit is 768 bytes, counted in bytes not characters', () => {
  ok('a'.repeat(768));
  bad('a'.repeat(769));
  // 'é' is 2 bytes in UTF-8: 384 of them are exactly at the limit, 385 are over it.
  ok('é'.repeat(384));
  bad('é'.repeat(385));
});

test('limits are overridable for tests', () => {
  const tiny = makeLimits({ MAX_PATH_DEPTH: 2, MAX_PATH_BYTES: 5 });
  ok('a/b', tiny);
  bad('a/b/c', tiny);
  bad('abcdef', tiny);
});

test('segments and joinPath treat root as empty', () => {
  assert.deepEqual(segments(''), []);
  assert.deepEqual(segments('a/b'), ['a', 'b']);
  assert.equal(joinPath('', 'a/b'), 'a/b');
  assert.equal(joinPath('a', ''), 'a');
  assert.equal(joinPath('a', 'b/c'), 'a/b/c');
  assert.equal(joinPath('', ''), '');
});
