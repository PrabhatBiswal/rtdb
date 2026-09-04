-- §8's schema, applied idempotently at startup into the connection's search_path schema.
-- No migration framework: CREATE ... IF NOT EXISTS is the whole story for v1 (WORKLOAD §2).

CREATE TABLE IF NOT EXISTS oplog (
  rev      BIGINT PRIMARY KEY,
  path     TEXT        NOT NULL,
  op       SMALLINT    NOT NULL,   -- 0 = put, 1 = merge (see OP_CODE in postgres.ts)
  value    JSONB,                  -- JSON null (a delete) is stored as jsonb 'null', never SQL NULL
  write_id UUID        NOT NULL UNIQUE,   -- §4 step 4: dedup IS this unique index
  ts       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- §8's two relevance shapes, both index-served:
--   descendant: path LIKE 'p/%' AND rev > R   -> needs text_pattern_ops (the DB collation is not C)
--   ancestor:   path = ANY(<=33 expanded ancestors) AND rev > R
CREATE INDEX IF NOT EXISTS oplog_path_pattern_rev ON oplog (path text_pattern_ops, rev);
CREATE INDEX IF NOT EXISTS oplog_path_rev         ON oplog (path, rev);

-- The materialized present: flattened leaf paths, kept prefix-free. Updated in the SAME txn as the
-- oplog insert (§8). Never pruned — it is not history.
CREATE TABLE IF NOT EXISTS nodes (
  path  TEXT PRIMARY KEY,
  value JSONB  NOT NULL,
  rev   BIGINT NOT NULL
);
-- Subtree reads and prefix-free maintenance are both `path LIKE 'p/%'`; the PK's default ops cannot
-- serve that under a non-C collation, so the pattern index is what makes a snapshot an index scan.
CREATE INDEX IF NOT EXISTS nodes_path_pattern ON nodes (path text_pattern_ops);

-- One row, shard 0. epoch and pruned_through live here because §2 requires the generation to be
-- persisted WITH the data, and §9's watermark must advance in the same txn as the prune (mentor
-- ruling 2026-08-28).
CREATE TABLE IF NOT EXISTS rev_counter (
  shard          INT PRIMARY KEY,
  v              BIGINT NOT NULL,
  epoch          BIGINT NOT NULL,
  pruned_through BIGINT NOT NULL
);
