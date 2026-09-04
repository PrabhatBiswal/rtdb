import type { ClientFrame, ErrCode, Json, Merge } from './frames.ts';
import type { Limits } from './limits.ts';
import { joinPath, validatePath } from './path.ts';

/**
 * Outcome of parsing one client->server frame.
 *  - frame:  validated, with unknown fields stripped (§Transport: unknown fields are ignored).
 *  - ignore: unknown frame type — MUST NOT be an error (§Transport). Covers v2's `reauth` (§2).
 *  - reject: send an err; writeId/subId scope it so the client can clear the right pending entry.
 */
export type ParseResult =
  | { kind: 'frame'; frame: ClientFrame }
  | { kind: 'ignore'; reason: string }
  | { kind: 'reject'; code: ErrCode; msg: string; writeId?: string; subId?: number };

const isObj = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);

const isInt = (v: unknown, min: number): v is number =>
  typeof v === 'number' && Number.isInteger(v) && v >= min;

const isStr = (v: unknown): v is string => typeof v === 'string';

/** §1 writeId is a UUIDv4; we accept any RFC-4122 shape (Phase 4 stores it in a UUID column). */
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const bad = (msg: string, scope?: { writeId?: string; subId?: number }): ParseResult => ({
  kind: 'reject',
  code: 'BADFRAME',
  msg,
  ...scope,
});

const badPath = (msg: string, scope?: { writeId?: string; subId?: number }): ParseResult => ({
  kind: 'reject',
  code: 'BADPATH',
  msg,
  ...scope,
});

/** Parse + validate one client->server frame. Frame-size enforcement is the socket layer's job (§9). */
export function parseClientFrame(text: string, limits: Limits): ParseResult {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return bad('malformed JSON');
  }
  if (!isObj(raw)) return bad('frame must be a JSON object');
  const type = raw['type'];
  if (!isStr(type)) return bad('frame missing string `type`');

  switch (type) {
    case 'hello': {
      if (!isInt(raw['proto'], 1)) return bad('hello.proto must be a positive integer');
      if (!isStr(raw['token'])) return bad('hello.token must be a string');
      const sdk = raw['sdk'];
      return {
        kind: 'frame',
        frame: { type: 'hello', proto: raw['proto'], token: raw['token'], ...(isStr(sdk) ? { sdk } : {}) },
      };
    }

    case 'listen': {
      const subId = raw['subId'];
      if (!isInt(subId, 1)) return bad('listen.subId must be a positive integer');
      const pathErr = validatePath(raw['path'], limits);
      if (pathErr) return badPath(pathErr, { subId });
      // §3: "no/zero lastRev -> snapshot", so a 0 is normalized away to absent here.
      const lastRev = raw['lastRev'];
      if (lastRev !== undefined && !isInt(lastRev, 0)) {
        return bad('listen.lastRev must be a non-negative integer', { subId });
      }
      return {
        kind: 'frame',
        frame: {
          type: 'listen',
          subId,
          path: raw['path'] as string,
          ...(isInt(lastRev, 1) ? { lastRev } : {}),
        },
      };
    }

    case 'unlisten': {
      const subId = raw['subId'];
      if (!isInt(subId, 1)) return bad('unlisten.subId must be a positive integer');
      return { kind: 'frame', frame: { type: 'unlisten', subId } };
    }

    case 'put':
    case 'merge':
    case 'cas': {
      const writeId = raw['writeId'];
      if (!isStr(writeId) || !UUID.test(writeId)) return bad(`${type}.writeId must be a UUID`);
      const pathErr = validatePath(raw['path'], limits);
      if (pathErr) return badPath(pathErr, { writeId });
      const path = raw['path'] as string;
      if (!('value' in raw)) return bad(`${type}.value is required`, { writeId });
      const value = raw['value'] as Json;

      if (type === 'put') return { kind: 'frame', frame: { type: 'put', writeId, path, value } };

      if (type === 'merge') {
        if (!isObj(value)) return bad('merge.value must be an object', { writeId });
        // Keys may be deep relative paths; each resolved child path must be legal on its own (§4).
        for (const key of Object.keys(value)) {
          if (key === '') return badPath('merge.value has an empty key', { writeId });
          const childErr = validatePath(joinPath(path, key), limits);
          if (childErr) return badPath(`merge key "${key}": ${childErr}`, { writeId });
        }
        return { kind: 'frame', frame: { type: 'merge', writeId, path, value: value as Merge['value'] } };
      }

      const expectedRev = raw['expectedRev'];
      if (!isInt(expectedRev, 0)) return bad('cas.expectedRev must be a non-negative integer', { writeId });
      return { kind: 'frame', frame: { type: 'cas', writeId, path, expectedRev, value } };
    }

    case 'ping':
    case 'pong': {
      const t = raw['t'];
      if (typeof t !== 'number' || !Number.isFinite(t)) return bad(`${type}.t must be a number`);
      return { kind: 'frame', frame: { type, t } };
    }

    default:
      // §Transport: unknown frame types MUST be ignored, not errored. Includes v2's `reauth` (§2).
      return { kind: 'ignore', reason: `unknown frame type "${type}"` };
  }
}
