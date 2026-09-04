/**
 * PROTOCOL.md §1–§10 frame types (v1 CORE).
 *
 * Wire rules that shape everything here (§Transport):
 *  - every frame is ONE JSON object with a `type` field;
 *  - unknown FIELDS are ignored (we never echo or store them);
 *  - unknown frame TYPES are ignored, never errors (this is how §11 Extensions ship later).
 * §11 Extensions (get/queries/push-id/serverTime/incr) are deliberately absent.
 */

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** §3 sub-scoped err codes: RULES, BADPATH, TOOBIG. §4 write err codes: all of the below. */
export type ErrCode = 'AUTH' | 'RULES' | 'BADPATH' | 'BADFRAME' | 'TOOBIG' | 'RATE';

/** WebSocket close codes (§2, §10). */
export const CLOSE = {
  /** frame sent before hello (§2). */
  PRE_HELLO: 4400,
  /** token rejected (§2). */
  AUTH: 4401,
  /** admin kick (§10). */
  KICK: 4403,
} as const;

// ---------------------------------------------------------------- client -> server

/** §2 */
export interface Hello {
  type: 'hello';
  proto: number;
  token: string;
  sdk?: string;
}

/** §3 — lastRev absent/0 means "send me a snapshot". */
export interface Listen {
  type: 'listen';
  subId: number;
  path: string;
  lastRev?: number;
}

/** §3 — no reply; in-flight deltas for it are dropped. */
export interface Unlisten {
  type: 'unlisten';
  subId: number;
}

/** §4 — wire form of BOTH setValue() and removeValue() (value:null deletes). */
export interface Put {
  type: 'put';
  writeId: string;
  path: string;
  value: Json;
}

/** §4 — keys may be deep relative paths; all children commit atomically under ONE rev. */
export interface Merge {
  type: 'merge';
  writeId: string;
  path: string;
  value: { [k: string]: Json };
}

/** §4 — commits iff the oplog has no relevant entry with rev > expectedRev for that path. */
export interface Cas {
  type: 'cas';
  writeId: string;
  path: string;
  expectedRev: number;
  value: Json;
}

/** §5 — `t` is echoed verbatim; clocks are never compared across machines. */
export interface Ping {
  type: 'ping';
  t: number;
}

export interface Pong {
  type: 'pong';
  t: number;
}

export type WriteFrame = Put | Merge | Cas;
export type ClientFrame = Hello | Listen | Unlisten | WriteFrame | Ping | Pong;

// ---------------------------------------------------------------- server -> client

/** §2 */
export interface HelloAck {
  type: 'helloAck';
  rev: number;
  /**
   * §2 (v1.5): the shard's generation, persisted with its data. A DIFFERENT epoch than the client
   * stored means every rev it holds is from a dead generation -> wholesale drop, fresh snapshots.
   */
  epoch: number;
  region: string;
  session: string;
}

/** §3 — client replaces the sub's serverState and sets sub.lastRev = rev. */
export interface Snapshot {
  type: 'snapshot';
  subId: number;
  path: string;
  value: Json;
  rev: number;
}

/** §3 — NO subId: a delta is encoded once and broadcast; clients route it by path. */
export interface Delta {
  type: 'delta';
  rev: number;
  path: string;
  op: 'put' | 'merge';
  value: Json;
}

/** §3 — micro-batching wire form; inner frames processed in array order. */
export interface Batch {
  type: 'batch';
  frames: ServerFrame[];
}

/** §3 — "your subscription went stale server-side"; a fresh snapshot follows. */
export interface Resync {
  type: 'resync';
  subId: number;
}

/** §4 — identical for first commit and duplicate replay; the client cannot and need not tell. */
export interface Ack {
  type: 'ack';
  writeId: string;
  rev: number;
}

/** §4 — a normal outcome, not an error; carries fresh state. */
export interface CasFail {
  type: 'casFail';
  writeId: string;
  path: string;
  value: Json;
  rev: number;
}

/** §2/§3/§4 — connection-scoped when bare, sub-scoped with subId, write-scoped with writeId. */
export interface Err {
  type: 'err';
  code: ErrCode;
  msg: string;
  subId?: number;
  writeId?: string;
}

export type ServerFrame = HelloAck | Snapshot | Delta | Batch | Resync | Ack | CasFail | Err | Pong;

// ---------------------------------------------------------------- admin plane (§10)

/** Out-of-band (Redis) -> gateways. Gateways close matching connections with CLOSE.KICK. */
export interface Kick {
  type: 'kick';
  target: { userId: string };
  reason?: string;
}
