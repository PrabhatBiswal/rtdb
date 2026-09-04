-- Wait-event sampler for the intermittent suite hang (WP8 Gate C §6).
--
-- Rows are emitted ONLY for backends that are doing something or are blocked, so an idle pool does
-- not fill the file. `pg_blocking_pids` is the column that matters: if the hang is rev_counter row
-- serialization, a blocked backend names the one holding the row, and the pair's query texts say
-- which statements they are.
--
-- ONE long-lived session with \watch rather than a psql per poll: the sampler must not perturb the
-- thing it measures, and forking five processes a second to look at a lock is not "not perturbing".
SELECT
  clock_timestamp()::text                                        AS t,
  pid,
  state,
  coalesce(wait_event_type, '-')                                 AS wtype,
  coalesce(wait_event, '-')                                      AS wevent,
  coalesce(array_to_string(pg_blocking_pids(pid), ','), '')       AS blocked_by,
  left(regexp_replace(coalesce(query, ''), '\s+', ' ', 'g'), 180) AS q
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND (state <> 'idle' OR cardinality(pg_blocking_pids(pid)) > 0)
ORDER BY pid
\watch 0.2
