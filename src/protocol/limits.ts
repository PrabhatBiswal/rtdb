/** PROTOCOL.md §9 — server defaults for v1. Override per-instance in tests. */
export interface Limits {
  /** Max client->server frame size, bytes (§9). */
  FRAME_MAX: number;
  /** Max server->client snapshot size, bytes; beyond -> sub-scoped TOOBIG (§9). */
  SNAPSHOT_MAX: number;
  /** Max flattened leaves in one write; beyond -> TOOBIG (§9). */
  MAX_LEAVES_PER_WRITE: number;
  /** Max path segments (§1, §9). */
  MAX_PATH_DEPTH: number;
  /** Max path length, bytes (§1, §9). */
  MAX_PATH_BYTES: number;
  /** Max relevant oplog entries served as catch-up deltas; beyond -> fresh snapshot (§3, §9). */
  CATCHUP_LIMIT: number;
  /** Oplog retention: whichever bound is hit first (§9). */
  OPLOG_RETENTION_MS: number;
  OPLOG_RETENTION_REVS: number;
  /** Per-connection write rate; beyond -> RATE (§9). */
  WRITE_RATE_PER_SEC: number;
  WRITE_RATE_BURST: number;
  /** Group-commit window for put/merge, ms (§4 step 2; WORKLOAD §4). */
  GROUP_COMMIT_MS: number;
  /** Delta micro-batch window, ms — engaged only on a non-empty send queue (§3 batch; WORKLOAD §4). */
  DELTA_BATCH_MS: number;
  /** Server WebSocket idle timeout, seconds (§5). */
  IDLE_TIMEOUT_SEC: number;
  /** Client pong timeout, ms (§5). */
  PONG_TIMEOUT_MS: number;
  /** Client ping intervals, ms — foreground / backgrounded (§5). */
  PING_FG_MS: number;
  PING_BG_MS: number;
  /**
   * Per-connection outbound buffer, bytes, beyond which the subscription is declared stale and
   * repaired with resync + fresh snapshot (§3). Operational knob, not a §9 limit.
   */
  SEND_QUEUE_MAX: number;
  /** Reconnect full-jitter backoff cap, ms (§6). */
  BACKOFF_CAP_MS: number;
  /** Stable-connection duration after which the backoff attempt counter resets, ms (§6). */
  BACKOFF_RESET_MS: number;
}

export const DEFAULT_LIMITS: Limits = {
  FRAME_MAX: 1024 * 1024,
  SNAPSHOT_MAX: 4 * 1024 * 1024,
  MAX_LEAVES_PER_WRITE: 2000,
  MAX_PATH_DEPTH: 32,
  MAX_PATH_BYTES: 768,
  CATCHUP_LIMIT: 500,
  OPLOG_RETENTION_MS: 2 * 60 * 60 * 1000,
  OPLOG_RETENTION_REVS: 500_000,
  WRITE_RATE_PER_SEC: 100,
  WRITE_RATE_BURST: 500,
  GROUP_COMMIT_MS: 5,
  DELTA_BATCH_MS: 20,
  IDLE_TIMEOUT_SEC: 70,
  PONG_TIMEOUT_MS: 10_000,
  PING_FG_MS: 25_000,
  PING_BG_MS: 60_000,
  SEND_QUEUE_MAX: 1024 * 1024,
  BACKOFF_CAP_MS: 30_000,
  BACKOFF_RESET_MS: 30_000,
};

export const makeLimits = (overrides: Partial<Limits> = {}): Limits => ({ ...DEFAULT_LIMITS, ...overrides });
