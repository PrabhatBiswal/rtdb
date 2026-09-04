import assert from 'node:assert/strict';
import test from 'node:test';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { parseClientFrame, type ParseResult } from '../../src/protocol/validate.ts';

const W = '0d0e1f2a-3b4c-4d5e-8f90-a1b2c3d4e5f6';
const parse = (v: unknown): ParseResult =>
  parseClientFrame(typeof v === 'string' ? v : JSON.stringify(v), DEFAULT_LIMITS);

const frame = (v: unknown) => {
  const r = parse(v);
  assert.equal(r.kind, 'frame', `expected a frame, got ${JSON.stringify(r)}`);
  return (r as Extract<ParseResult, { kind: 'frame' }>).frame;
};
const reject = (v: unknown) => {
  const r = parse(v);
  assert.equal(r.kind, 'reject', `expected a reject, got ${JSON.stringify(r)}`);
  return r as Extract<ParseResult, { kind: 'reject' }>;
};

// ---- §Transport tolerance rules -------------------------------------------------

test('unknown frame types are ignored, never errors', () => {
  for (const type of ['reauth', 'get', 'incr', 'somethingFromV3']) {
    assert.equal(parse({ type, whatever: 1 }).kind, 'ignore');
  }
});

test('unknown fields are ignored and stripped', () => {
  assert.deepEqual(frame({ type: 'ping', t: 7, futureField: { a: 1 } }), { type: 'ping', t: 7 });
  assert.deepEqual(frame({ type: 'hello', proto: 1, token: 'tk', sdk: 'kotlin/1.0.0', region: 'x' }), {
    type: 'hello',
    proto: 1,
    token: 'tk',
    sdk: 'kotlin/1.0.0',
  });
});

test('non-frames are BADFRAME', () => {
  for (const raw of ['{', 'null', '3', '"str"', '[{"type":"ping","t":1}]']) {
    assert.equal(reject(raw).code, 'BADFRAME');
  }
  assert.equal(reject({ noType: true }).code, 'BADFRAME');
  assert.equal(reject({ type: 9 }).code, 'BADFRAME');
});

// ---- §2 hello --------------------------------------------------------------------

test('hello requires proto and token; sdk is optional', () => {
  assert.deepEqual(frame({ type: 'hello', proto: 1, token: 'tk' }), { type: 'hello', proto: 1, token: 'tk' });
  reject({ type: 'hello', token: 'tk' });
  reject({ type: 'hello', proto: 1 });
  reject({ type: 'hello', proto: 0, token: 'tk' });
  reject({ type: 'hello', proto: 1, token: 5 });
});

// ---- §3 listen / unlisten --------------------------------------------------------

test('listen validates subId, path and lastRev', () => {
  assert.deepEqual(frame({ type: 'listen', subId: 7, path: 'a/b', lastRev: 184190 }), {
    type: 'listen',
    subId: 7,
    path: 'a/b',
    lastRev: 184190,
  });
  // §3: no/zero lastRev both mean "send a snapshot" — 0 normalizes to absent.
  assert.deepEqual(frame({ type: 'listen', subId: 7, path: '' }), { type: 'listen', subId: 7, path: '' });
  assert.deepEqual(frame({ type: 'listen', subId: 7, path: '', lastRev: 0 }), {
    type: 'listen',
    subId: 7,
    path: '',
  });
  reject({ type: 'listen', subId: 0, path: 'a' });
  reject({ type: 'listen', subId: 1.5, path: 'a' });
  reject({ type: 'listen', subId: 7, path: 'a', lastRev: -1 });
});

test('a bad listen path is BADPATH scoped to the subId', () => {
  const r = reject({ type: 'listen', subId: 7, path: 'a//b' });
  assert.equal(r.code, 'BADPATH');
  assert.equal(r.subId, 7);
});

test('unlisten needs only a subId', () => {
  assert.deepEqual(frame({ type: 'unlisten', subId: 7 }), { type: 'unlisten', subId: 7 });
  reject({ type: 'unlisten' });
});

// ---- §4 writes -------------------------------------------------------------------

test('put carries any JSON value, and null means delete', () => {
  assert.deepEqual(frame({ type: 'put', writeId: W, path: 'a', value: null }), {
    type: 'put',
    writeId: W,
    path: 'a',
    value: null,
  });
  assert.deepEqual(frame({ type: 'put', writeId: W, path: '', value: { n: 'Ravi' } }).type, 'put');
  // absent value is not the same as null
  assert.equal(reject({ type: 'put', writeId: W, path: 'a' }).code, 'BADFRAME');
});

test('writeId must be a UUID, and write rejects are scoped to it', () => {
  assert.equal(reject({ type: 'put', writeId: 'nope', path: 'a', value: 1 }).code, 'BADFRAME');
  assert.equal(reject({ type: 'put', writeId: W, path: 'a/', value: 1 }).writeId, W);
});

test('merge value must be an object; deep keys are validated as paths', () => {
  assert.deepEqual(frame({ type: 'merge', writeId: W, path: 'a', value: { 'stats/wins': 3, tag: null } }), {
    type: 'merge',
    writeId: W,
    path: 'a',
    value: { 'stats/wins': 3, tag: null },
  });
  reject({ type: 'merge', writeId: W, path: 'a', value: 5 });
  reject({ type: 'merge', writeId: W, path: 'a', value: [1, 2] });
  assert.equal(reject({ type: 'merge', writeId: W, path: 'a', value: { '': 1 } }).code, 'BADPATH');
  assert.equal(reject({ type: 'merge', writeId: W, path: 'a', value: { 'x.y': 1 } }).code, 'BADPATH');
  assert.equal(reject({ type: 'merge', writeId: W, path: 'a', value: { 'x//y': 1 } }).code, 'BADPATH');
});

test('cas requires expectedRev', () => {
  assert.deepEqual(frame({ type: 'cas', writeId: W, path: 'a/score', expectedRev: 184224, value: 51 }), {
    type: 'cas',
    writeId: W,
    path: 'a/score',
    expectedRev: 184224,
    value: 51,
  });
  reject({ type: 'cas', writeId: W, path: 'a', value: 1 });
  reject({ type: 'cas', writeId: W, path: 'a', expectedRev: -1, value: 1 });
});

// ---- §5 liveness -----------------------------------------------------------------

test('ping/pong require a numeric t', () => {
  assert.deepEqual(frame({ type: 'pong', t: 1756280000000 }), { type: 'pong', t: 1756280000000 });
  reject({ type: 'ping' });
  reject({ type: 'ping', t: 'now' });
});
