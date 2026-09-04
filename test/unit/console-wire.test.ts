import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

/**
 * §5.9 Gate B — what may leave the console, and how a typed value is read.
 *
 * EXTRACTED from the shipped HTML rather than reimplemented here, exactly as the mirror block is:
 * the console has no build step, so a copy in a test file is a copy that drifts, and console Gate B
 * already taught this project what a drifted copy costs. This evaluates the code the browser runs.
 *
 * Note what these tests are and are not. The outbound allowlist is a CONVENIENCE — it decides what
 * the page offers. The guard is the gateway's, and it has its own tests over a real socket
 * (test/integration/console-write.test.ts). A viewer who bypasses everything here is refused there.
 */
const HTML = fileURLToPath(new URL('../../console/rtdb-console.html', import.meta.url));

interface Wire {
  outboundAllowed(type: string, role: string): boolean;
  parseTypedValue(kind: string, raw: string): { ok: true; value: unknown } | { ok: false; msg: string };
  buildPut(path: string, value: unknown): { type: string; writeId: string; path: string; value: unknown };
  writeOutcomeMessage(f: Record<string, unknown> | null): { ok: boolean; text: string };
}

function loadWire(): Wire {
  const src = readFileSync(HTML, 'utf8');
  const block = /<script id="rtdb-wire">([\s\S]*?)<\/script>/.exec(src);
  assert.ok(block, 'the console must keep its outbound rules in a <script id="rtdb-wire"> block');
  const mod = { exports: {} as Wire };
  new Function('module', block[1] as string)(mod);
  return mod.exports;
}

test('a viewer session may read and may not write', () => {
  const { outboundAllowed } = loadWire();
  for (const f of ['hello', 'listen', 'unlisten', 'ping']) {
    assert.equal(outboundAllowed(f, 'viewer'), true, `${f} is a read frame`);
  }
  assert.equal(outboundAllowed('put', 'viewer'), false);
});

test('editor and owner may put; nobody may merge or cas from this page', () => {
  const { outboundAllowed } = loadWire();
  assert.equal(outboundAllowed('put', 'editor'), true);
  assert.equal(outboundAllowed('put', 'owner'), true);
  // Set, add-child and delete are all puts, so the narrower allowlist costs the feature nothing —
  // and every frame it does not admit is a frame this page cannot be tricked into constructing.
  for (const role of ['viewer', 'editor', 'owner']) {
    assert.equal(outboundAllowed('merge', role), false, `merge stays refused for ${role}`);
    assert.equal(outboundAllowed('cas', role), false, `cas stays refused for ${role}`);
  }
});

test('an unknown role writes nothing — including the empty one a signed-out page holds', () => {
  const { outboundAllowed } = loadWire();
  for (const role of ['', 'admin', 'root', 'undefined']) {
    assert.equal(outboundAllowed('put', role), false, `role "${role}" must not write`);
  }
});

test('the typed editor reads each type, and says why when it cannot', () => {
  const { parseTypedValue } = loadWire();
  assert.deepEqual(parseTypedValue('string', 'hello'), { ok: true, value: 'hello' });
  assert.deepEqual(parseTypedValue('string', ''), { ok: true, value: '' });
  assert.deepEqual(parseTypedValue('number', ' 42 '), { ok: true, value: 42 });
  assert.deepEqual(parseTypedValue('number', '-1.5'), { ok: true, value: -1.5 });
  assert.deepEqual(parseTypedValue('boolean', 'TRUE'), { ok: true, value: true });
  assert.deepEqual(parseTypedValue('boolean', 'false'), { ok: true, value: false });
  assert.deepEqual(parseTypedValue('json', '{"a":[1,null]}'), { ok: true, value: { a: [1, null] } });
  // §4: the delete IS a put of null, so the type selector is where a delete can also be spelled.
  assert.deepEqual(parseTypedValue('null', 'ignored'), { ok: true, value: null });

  for (const [kind, raw] of [['number', 'twelve'], ['number', ''], ['boolean', 'yes'], ['json', '{oops'], ['nope', 'x']]) {
    const r = parseTypedValue(kind as string, raw as string);
    assert.equal(r.ok, false, `${kind}/${raw} must not parse`);
    assert.ok((r as { msg: string }).msg.length > 0, 'a refusal explains itself');
  }
});

test('a number that is not finite is refused rather than sent as null', () => {
  // JSON.stringify(Infinity) is "null", so a value that slipped through here would silently become
  // a DELETE on the wire. That is the worst possible mistranslation of a typed editor.
  const { parseTypedValue } = loadWire();
  for (const raw of ['Infinity', '-Infinity', 'NaN']) {
    assert.equal(parseTypedValue('number', raw).ok, false, `${raw} must not parse as a number`);
  }
});

test('every put carries its own UUIDv4 writeId (§4)', () => {
  const { buildPut } = loadWire();
  const a = buildPut('demo/x', 1);
  const b = buildPut('demo/x', 1);
  assert.equal(a.type, 'put');
  assert.equal(a.path, 'demo/x');
  assert.equal(a.value, 1);
  assert.match(a.writeId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.notEqual(a.writeId, b.writeId, 'two writes are two writeIds');
});

/**
 * The mentors' addition: the page may believe a write is allowed while the wire disagrees — a
 * demoted editor holding a stale tab is the ordinary case. The refusal must SAY so where the person
 * is looking. A refusal that renders as silence is indistinguishable from a save that worked.
 */
test('a RULES refusal is rendered, and names what to do about it', () => {
  const { writeOutcomeMessage } = loadWire();
  const r = writeOutcomeMessage({ type: 'err', code: 'RULES', msg: 'this console session may not write' });
  assert.equal(r.ok, false);
  assert.match(r.text, /refused by the server/i);
  assert.match(r.text, /RULES/);
  assert.match(r.text, /sign in again/i, 'the operator is told what changed under them');
});

test('every write outcome says something — including no answer at all', () => {
  const { writeOutcomeMessage } = loadWire();
  const ack = writeOutcomeMessage({ type: 'ack', rev: 41 });
  assert.equal(ack.ok, true);
  assert.match(ack.text, /41/);

  for (const f of [
    null,
    { type: 'err', code: 'BADPATH', msg: 'bad path' },
    { type: 'err', code: 'TOOBIG', msg: 'too big' },
    { type: 'casFail' },
    { type: 'something-new' },
  ]) {
    const r = writeOutcomeMessage(f as Record<string, unknown> | null);
    assert.equal(r.ok, false, `${JSON.stringify(f)} is not a success`);
    assert.ok(r.text.trim().length > 0, `${JSON.stringify(f)} must not render as silence`);
  }
});
