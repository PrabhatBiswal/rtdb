package com.hobostays.rtdb.demo

import java.util.Base64
import java.util.concurrent.Executor
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * WORKLOAD §5.7.4's teeth. Every one of these fails if the mechanism it names is removed — which is
 * the only property that makes a test worth the line count (WP7's standing rule).
 *
 * There is no server, no device and no waiting here: the clock is a variable and the endpoint is a
 * lambda, which is the entire reason [TokenSource] has no timer of its own.
 */
class TokenSourceTest {

    /** Runs the fetch on the calling thread, so a test asserts after `tick()` returns. */
    private val direct = Executor { it.run() }
    private var now = 1_700_000_000_000L

    private fun jwt(expSeconds: Long?, sub: String = "shadow-pixel"): String {
        val b64 = { s: String -> Base64.getUrlEncoder().withoutPadding().encodeToString(s.toByteArray()) }
        val claims = if (expSeconds == null) """{"sub":"$sub"}""" else """{"sub":"$sub","exp":$expSeconds}"""
        return "${b64("""{"alg":"HS256","typ":"JWT"}""")}.${b64(claims)}.c2ln"
    }

    // ------------------------------------------------------------------ exp parsing

    @Test
    fun `exp parsing reads the claim a real shadow token carries`() {
        assertEquals(1_700_086_400L, TokenSource.jwtExpSeconds(jwt(1_700_086_400L)))
    }

    @Test
    fun `exp parsing survives every shape that is not a token, without throwing`() {
        // An app that crashes on a malformed token has replaced an auth problem with a crash.
        listOf(
            "",
            "not-a-token",
            "only.two",
            "a.b.c.d",
            "${Base64.getUrlEncoder().withoutPadding().encodeToString("{}".toByteArray())}.@@@@.sig",
            jwt(null),                                    // a JWT with no exp claim at all
            "head.${Base64.getUrlEncoder().withoutPadding().encodeToString("""{"exp":"soon"}""".toByteArray())}.sig",
        ).forEach { assertNull("expected null for <$it>", TokenSource.jwtExpSeconds(it)) }
    }

    @Test
    fun `exp parsing does not require padding, which base64url does not carry`() {
        // The server mints with base64url and strips '='. A decoder that demands padding reads
        // every third token as garbage — intermittently, which is the worst way to find out.
        val token = jwt(1_700_086_400L)
        assertTrue("fixture should be unpadded", !token.contains("="))
        assertEquals(1_700_086_400L, TokenSource.jwtExpSeconds(token))
    }

    // ------------------------------------------------------------------ scheduling

    @Test
    fun `scheduling refreshes a minute BEFORE exp, not at it`() {
        val exp = now / 1000 + 86_400
        assertEquals(exp * 1000 - 60_000, TokenSource.refreshAt(now, exp, 0, 60_000))
    }

    @Test
    fun `scheduling lets the test cap shorten a lifetime but never extend one`() {
        val exp = now / 1000 + 86_400
        // Cap shorter than exp: the cap wins — this is what puts a live refresh on screen in 90s.
        assertEquals(now + 90_000, TokenSource.refreshAt(now, exp, 90_000, 60_000))
        // Cap LONGER than exp: exp still wins. The hook cannot be used to outlive a token.
        assertEquals(exp * 1000 - 60_000, TokenSource.refreshAt(now, exp, 172_800_000, 60_000))
    }

    @Test
    fun `scheduling keeps using a token whose exp it cannot read, and re-checks later`() {
        assertEquals(now + TokenSource.UNKNOWN_LIFETIME_MS, TokenSource.refreshAt(now, null, 0, 60_000))
    }

    @Test
    fun `scheduling never lands in the past, however late the token already is`() {
        // A clock two minutes fast makes a fresh 24h token look like it is inside its skew window.
        // Without the floor this returns a time already gone, and the app refetches every tick.
        val expiredExp = now / 1000 - 5_000
        assertEquals(now + TokenSource.MIN_REFRESH_GAP_MS, TokenSource.refreshAt(now, expiredExp, 0, 60_000))
    }

    // ------------------------------------------------------------------ the tick

    @Test
    fun `tick fetches once at launch and then not again until the refresh point`() {
        var fetches = 0
        val exp = now / 1000 + 3_600
        val source = source(fetch = { fetches++; jwt(exp) })

        source.tick()
        assertEquals(1, fetches)
        val held = source.state as TokenSource.State.Held
        assertEquals(exp * 1000 - 60_000, held.refreshAtMs)

        now = held.refreshAtMs - 1
        source.tick()
        assertEquals("a tick one millisecond early must not fetch", 1, fetches)

        now = held.refreshAtMs
        source.tick()
        assertEquals("the proactive refresh is the whole point of §5.7.3", 2, fetches)
    }

    @Test
    fun `tick hands every fresh token to the client, which is what reconnects it`() {
        val handed = mutableListOf<String>()
        val exp = now / 1000 + 3_600
        val source = source(fetch = { jwt(exp) }, onToken = { handed += it })
        source.tick()
        assertEquals(listOf(jwt(exp)), handed)
    }

    // ------------------------------------------------------------------ the 4401

    @Test
    fun `a 4401 refetches even though exp says the token is still good`() {
        // The server is the authority, not our reading of the payload. Without invalidate() the
        // app sits in CLOSED holding a token it believes in for another 23 hours.
        var fetches = 0
        val source = source(fetch = { fetches++; jwt(now / 1000 + 86_400) })
        source.tick()
        assertEquals(1, fetches)

        source.tick()
        assertEquals("nothing is due yet", 1, fetches)

        source.invalidate("4401 unauthorized")
        now += TokenSource.MIN_REFRESH_GAP_MS + 60_000 // past the first backoff
        source.tick()
        assertEquals(2, fetches)
    }

    @Test
    fun `a 4401 arriving mid-fetch does not start a second one`() {
        // The race an eager refetch-on-4401 walks into: the socket dies while a replacement token
        // is ALREADY in flight, and the app ends up with two fetches and two connections.
        //
        // This tooth was false-green on its first run, and the reason is worth keeping: it used a
        // direct executor, which runs the fetch to completion inside tick() and makes the very
        // concurrency it claims to test impossible to express. The fetch has to be genuinely held
        // in flight — hence the queue.
        val inFlight = mutableListOf<Runnable>()
        var fetches = 0
        val source = source(io = { inFlight += it }, fetch = { fetches++; jwt(now / 1000 + 86_400) })

        source.tick()
        assertEquals("queued, not run: this is what 'in flight' means", 0, fetches)

        source.invalidate("4401 while a fetch was already in flight")
        now += 60_000 // past any backoff the invalidate might have scheduled
        source.tick() // without the guard this starts a SECOND fetch alongside the first

        inFlight.forEach { it.run() }
        assertEquals("one fetch in flight means one fetch", 1, fetches)
        assertTrue("and the in-flight result stands", source.state is TokenSource.State.Held)
    }

    @Test
    fun `a failing endpoint backs off instead of hammering it`() {
        var fetches = 0
        val source = source(fetch = { fetches++; throw java.io.IOException("shadow-token HTTP 503") })

        source.tick()
        val first = (source.state as TokenSource.State.Failed)
        assertTrue("the reason belongs on screen", first.why.contains("503"))

        now += 1
        source.tick()
        assertEquals("a tick inside the backoff window must not retry", 1, fetches)
        now -= 1

        // §6's curve, the same one the SDK reconnects on: 1s, 2s, 4s … capped at 30s.
        assertEquals(1_000L, first.retryAtMs - now)

        now = first.retryAtMs
        source.tick()
        assertEquals(2, fetches)
        assertEquals(2_000L, (source.state as TokenSource.State.Failed).retryAtMs - now)
    }

    @Test
    fun `a failure is not terminal — a later tick recovers and clears the backoff`() {
        var fail = true
        var fetches = 0
        val source = source(fetch = {
            fetches++
            if (fail) throw java.io.IOException("network unreachable") else jwt(now / 1000 + 3_600)
        })
        source.tick()
        now = (source.state as TokenSource.State.Failed).retryAtMs
        fail = false
        source.tick()
        assertEquals(2, fetches)
        assertTrue(source.state is TokenSource.State.Held)

        // And the backoff counter reset with the success: the next failure waits the SHORT delay
        // again, not the long one it had climbed to.
        fail = true
        source.invalidate("4401")
        assertTrue((source.state as TokenSource.State.Failed).retryAtMs - now <= 1_000)
    }

    // ------------------------------------------------------------------ no key

    @Test
    fun `an unconfigured source never calls the endpoint, and says so instead of crashing`() {
        var fetches = 0
        val source = source(fetch = { fetches++; jwt(now / 1000 + 3_600) })
        source.unconfigured()
        repeat(10) { now += 3_600_000; source.tick() }
        source.invalidate("4401")
        assertEquals(0, fetches)
        assertEquals(TokenSource.State.Unconfigured, source.state)
    }

    // ------------------------------------------------------------------ harness

    private fun source(
        fetch: (String) -> String,
        onToken: (String) -> Unit = {},
        io: Executor = direct,
        lifetimeCapMs: Long = 0,
    ) = TokenSource(
        device = "pixel",
        fetch = fetch,
        onToken = onToken,
        clock = { now },
        io = io,
        lifetimeCapMs = lifetimeCapMs,
        skewMs = 60_000,
        jitter = { 1.0 }, // full delay, so backoff is a fixed number instead of a range
    )
}
