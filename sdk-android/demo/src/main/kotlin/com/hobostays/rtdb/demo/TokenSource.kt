package com.hobostays.rtdb.demo

import com.hobostays.rtdb.core.Limits
import com.hobostays.rtdb.core.backoffDelay
import java.io.ByteArrayOutputStream
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

/**
 * WORKLOAD §5.7: the app holds its own token, forever.
 *
 * **This file is the reference implementation for an app's token flow — it is written to be
 * copied.** Everything specific to this demo (the endpoint, the key, the UI) is a constructor
 * argument; what is left is the part any app needs, and it is deliberately boring.
 *
 * The shape, and why it is this shape:
 *
 * 1. **No timer.** A token source that owns a scheduler is a thing to start, stop, leak and mock.
 *    This one is a state machine with a [tick] the app calls from a cadence it ALREADY has — the
 *    demo's 5s status handler, an app's own heartbeat, anything. Five seconds of granularity on a
 *    24-hour token is not a compromise, and the whole scheduling decision becomes a pure function
 *    ([refreshAt]) that a test can drive with a fake clock instead of a real wait.
 * 2. **Both directions.** Proactive: refresh before `exp`, so a healthy app never sees a 4401 at
 *    all. Reactive: [invalidate] on the 4401 that arrives anyway — because the clock can be wrong,
 *    the token can be revoked (§10 kick), and the server is the only authority on either. An app
 *    that has only the proactive half works until the first time it matters.
 * 3. **The fetch is injected.** The I/O is one lambda, so the decision logic tests without a
 *    server and the endpoint can be anything the app already talks to.
 *
 * Threading: [tick] and [invalidate] are safe from any thread; the fetch runs on [io], never the
 * caller's thread. [state] is @Volatile for the UI to read on the main thread.
 */
class TokenSource(
    private val device: String,
    /** Blocking. Returns a JWT, or throws with a message a human can act on. */
    private val fetch: (device: String) -> String,
    /** Called with each fresh token, off the main thread. For the demo: `client.connect(token)`. */
    private val onToken: (String) -> Unit,
    private val clock: () -> Long = System::currentTimeMillis,
    private val io: Executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "rtdb-token").apply { isDaemon = true }
    },
    /**
     * §5.7.4's test hook: pretend every token expires this soon after it arrives, so the live
     * refresh can be watched on screen in a minute instead of in a day. 0 = trust `exp`, which is
     * what a shipping app does. It only ever SHORTENS a lifetime (see [refreshAt]) — it cannot be
     * used to keep a token past its real expiry.
     */
    private val lifetimeCapMs: Long = 0,
    private val skewMs: Long = DEFAULT_SKEW_MS,
    /** [0,1) roll for §6's full-jitter backoff. Fixed in tests. */
    private val jitter: () -> Double = Math::random,
) {
    sealed interface State {
        /** No key was configured at build time. Not an error and not a crash — a state with a name. */
        object Unconfigured : State
        /** Nothing fetched yet; the next tick will. */
        object Idle : State
        object Fetching : State
        data class Held(val refreshAtMs: Long, val expSeconds: Long?) : State
        data class Failed(val why: String, val retryAtMs: Long) : State
    }

    @Volatile
    var state: State = State.Idle
        private set

    /** Fetch attempts since the last success — §6's backoff counter, for the same reason. */
    @Volatile
    private var attempt = 0

    /** Marks the source permanently unconfigured. The app calls this when the key is missing. */
    fun unconfigured() {
        state = State.Unconfigured
    }

    /**
     * Adopt a token the app ALREADY has — cached from a previous launch, handed over by the app's
     * own login, baked into a debug build. Skips the fetch at launch and picks up the schedule from
     * that token's own `exp`, so a restart is not a round trip.
     *
     * A token whose `exp` cannot be read is adopted anyway, on [UNKNOWN_LIFETIME_MS] — deliberately:
     * this is the path a dead or malformed cached token takes, and letting the 4401 be what
     * discovers it is both correct and the only thing that works when a clock is wrong.
     */
    fun seed(token: String) {
        if (state !is State.Idle) return
        val exp = jwtExpSeconds(token)
        state = State.Held(refreshAt(clock(), exp, lifetimeCapMs, skewMs), exp)
    }

    /**
     * Call on any cadence. Fetches if it is time to; does nothing if it is not, and does nothing
     * at all once [unconfigured].
     */
    fun tick() {
        val now = clock()
        when (val s = state) {
            is State.Unconfigured, is State.Fetching -> return
            is State.Idle -> start()
            is State.Held -> if (now >= s.refreshAtMs) start()
            is State.Failed -> if (now >= s.retryAtMs) start()
        }
    }

    /**
     * The reactive half: the server has REJECTED what we hold (a 4401, or a §10 kick). Whatever we
     * think about `exp` is now wrong — the server is the authority — so drop it and refetch on the
     * next tick, under backoff so a permanently-bad key cannot become a hot loop.
     *
     * Not a fetch of its own: going through [state] means a 4401 arriving mid-fetch cannot start a
     * second one, which is the race an eager refetch-on-4401 walks straight into.
     */
    fun invalidate(why: String) {
        if (state is State.Unconfigured || state is State.Fetching) return
        state = State.Failed(why, clock() + backoffDelay(attempt, Limits(), jitter()))
        attempt++
    }

    private fun start() {
        state = State.Fetching
        io.execute {
            try {
                val token = fetch(device)
                val exp = jwtExpSeconds(token)
                attempt = 0
                state = State.Held(refreshAt(clock(), exp, lifetimeCapMs, skewMs), exp)
                onToken(token)
            } catch (e: Exception) {
                // Catch-all on purpose: an app must not die because a token endpoint had a bad day.
                // The reason is carried into the state so the screen can say WHICH bad day it was.
                val delay = backoffDelay(attempt, Limits(), jitter())
                attempt++
                state = State.Failed(e.message ?: e.javaClass.simpleName, clock() + delay)
            }
        }
    }

    companion object {
        /** Refresh a minute early: enough for a slow fetch and a modest clock skew. */
        const val DEFAULT_SKEW_MS = 60_000L

        /**
         * How long an UNREADABLE token is assumed to last. Not zero: a token whose `exp` we cannot
         * parse may still be perfectly valid (a different issuer, a claim we do not know), so the
         * app keeps using it and re-checks periodically rather than throwing it away. If it really
         * is dead the 4401 tells us in one round trip, which is what [invalidate] is for.
         */
        const val UNKNOWN_LIFETIME_MS = 5 * 60_000L

        /**
         * The floor under every schedule. Without it, a token that arrives already inside its skew
         * window — a clock a minute fast is enough — schedules a refresh in the past, and the app
         * refetches on every single tick for as long as that lasts. A minted-per-request endpoint
         * would answer all of them, so nothing would break loudly; it would just quietly hammer.
         */
        const val MIN_REFRESH_GAP_MS = 10_000L

        /**
         * The whole scheduling decision, as a pure function. Earliest of: a minute before `exp`,
         * and the test cap if one is set — never sooner than the floor.
         */
        fun refreshAt(nowMs: Long, expSeconds: Long?, lifetimeCapMs: Long, skewMs: Long): Long {
            val fromExp = expSeconds?.let { it * 1000 - skewMs } ?: (nowMs + UNKNOWN_LIFETIME_MS)
            val fromCap = if (lifetimeCapMs > 0) nowMs + lifetimeCapMs else Long.MAX_VALUE
            return maxOf(nowMs + MIN_REFRESH_GAP_MS, minOf(fromExp, fromCap))
        }

        /**
         * The `exp` claim in seconds, or null if this is not a JWT whose expiry we can read.
         *
         * The signature is NOT checked and must not be: only the server holds the secret, and an
         * app that could verify its own tokens would be an app carrying the secret. This reads the
         * payload as the hint it is — the authority on whether a token is good is the 4401.
         */
        fun jwtExpSeconds(jwt: String): Long? {
            // No shape check ahead of this: `runCatching` already turns every non-JWT into null,
            // including a string with no dots at all (`parts[1]` throws, and that is a failure to
            // parse like any other). A separate `parts.size != 3` guard read as defensive and was
            // provably dead — no mutation of it could make a test fail.
            return runCatching {
                val payload = String(base64UrlDecode(jwt.split('.')[1]), Charsets.UTF_8)
                Json.parseToJsonElement(payload).jsonObject["exp"]?.jsonPrimitive?.long
            }.getOrNull()
        }

        /**
         * base64url, by hand in ten lines, for one reason: `java.util.Base64` is API 26 and this
         * app supports 23, while `android.util.Base64` does not exist in a JVM unit test — and the
         * expiry parser is exactly the thing that has to be tested without a device.
         */
        private fun base64UrlDecode(s: String): ByteArray {
            val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            val out = ByteArrayOutputStream()
            var buffer = 0
            var bits = 0
            for (c in s) {
                val v = alphabet.indexOf(c)
                if (v < 0) continue // '=' padding and any stray whitespace
                buffer = (buffer shl 6) or v
                bits += 6
                if (bits >= 8) {
                    bits -= 8
                    out.write((buffer shr bits) and 0xff)
                }
            }
            return out.toByteArray()
        }
    }
}

/**
 * The one piece of I/O, kept out of [TokenSource] so the decisions above test without a server.
 * `POST /shadow-token` with the bearer key; the reply is `{token, sub, expiresInHours}`.
 *
 * `HttpURLConnection` rather than OkHttp: the SDK declares OkHttp `implementation`, so it reaches a
 * consumer at RUNTIME scope only and is not on this app's compile classpath — deliberately, since
 * an SDK should not force its HTTP client on the app that embeds it. One POST needs no client.
 *
 * The key is sent and never stored, never logged, and never put in a message: an error carries the
 * status and the server's own words, both of which are safe, and neither of which is the key.
 */
fun fetchShadowToken(url: String, key: String, device: String): String {
    val connection = (java.net.URL(url).openConnection() as java.net.HttpURLConnection).apply {
        requestMethod = "POST"
        setRequestProperty("authorization", "Bearer $key")
        setRequestProperty("content-type", "application/json")
        connectTimeout = 10_000
        readTimeout = 10_000
        doOutput = true
    }
    try {
        connection.outputStream.use { it.write(Json.encodeToString(mapOf("device" to device)).toByteArray()) }
        val code = connection.responseCode
        val body = (if (code in 200..299) connection.inputStream else connection.errorStream)
            ?.bufferedReader()?.use { it.readText() }.orEmpty()
        if (code !in 200..299) {
            // 401 here means the key in local.properties is wrong or rotated — the one failure an
            // operator can actually fix, so it says so instead of reading as a network blip.
            val hint = if (code == 401) " (the shadow key was rejected — check local.properties)" else ""
            throw java.io.IOException("shadow-token HTTP $code$hint: ${body.take(200)}")
        }
        return Json.parseToJsonElement(body).jsonObject["token"]?.jsonPrimitive?.content
            ?: throw java.io.IOException("shadow-token returned no token")
    } finally {
        connection.disconnect()
    }
}
