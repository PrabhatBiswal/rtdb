package com.hobostays.rtdb.core

/**
 * The client-side half of PROTOCOL §9's limits plus the §5/§6 timers, in one object tests can
 * override (WORKLOAD §4). Server-only limits (frame caps, catch-up, retention, rates) live in the
 * gateway and are deliberately absent here — the client learns about them through `err` frames.
 *
 * Names are camelCase Kotlin rather than the TypeScript SCREAMING_CASE; the values are the §9/§5/§6
 * defaults verbatim.
 */
data class Limits(
    /** §1/§9 path shape. */
    val maxPathDepth: Int = 32,
    val maxPathBytes: Int = 768,
    /** §5 ping cadence: 25s foreground, 60s backgrounded. */
    val pingForegroundMs: Long = 25_000,
    val pingBackgroundMs: Long = 60_000,
    /** §5: no pong within this and the connection is closed and retried. */
    val pongTimeoutMs: Long = 10_000,
    /**
     * How long the socket may stay open with no readable `helloAck` before the connection is
     * treated as dead (§2). Deliberately NOT [pongTimeoutMs]: a ping is a pure echo (§5), while
     * hello does real work — token validation and the shard/epoch lookup — so the two are not the
     * same round trip and must not share a number. Comfortably under §6's 30s [backoffCapMs] /
     * [backoffResetMs], the horizon this project already treats as "definitely failed", so a stuck
     * hello is diagnosed and retried well inside one worst-case backoff rather than hidden by it.
     */
    val helloTimeoutMs: Long = 15_000,
    /** §6 full-jitter backoff cap. */
    val backoffCapMs: Long = 30_000,
    /** §6: how long a connection must hold before the attempt counter resets. */
    val backoffResetMs: Long = 30_000,
)
