package com.hobostays.rtdb.core

import com.hobostays.rtdb.FakeTransport
import com.hobostays.rtdb.awaitState
import com.hobostays.rtdb.FixedRandom
import com.hobostays.rtdb.RecordingListener
import com.hobostays.rtdb.sentOf
import com.hobostays.rtdb.waitUntil
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

/**
 * §2/§5/§6 FSM behaviour driven through a fake socket, for the cases a real server cannot be made
 * to produce on demand. Everything that CAN be driven for real is, in LifecycleIntegrationTest.
 */
class ConnectionTest {

    private fun helloAck(epoch: Long = 1, rev: Long = 0) =
        """{"type":"helloAck","rev":$rev,"epoch":$epoch,"region":"test","session":"s_fake"}"""

    private fun fixture(
        limits: Limits = Limits(backoffCapMs = 20, backoffResetMs = 30_000),
        pingIntervalMs: Long = 60_000,
        pongTimeoutMs: Long = 50,
        autoReconnect: Boolean = true,
    ): Triple<Connection, FakeTransport, RecordingListener> {
        val wire = FakeTransport()
        val recorder = RecordingListener()
        val connection = Connection(
            ConnectionOptions(
                url = "ws://fake",
                token = "t",
                sdk = "kotlin/0.1.0",
                limits = limits,
                pingIntervalMs = pingIntervalMs,
                pongTimeoutMs = pongTimeoutMs,
                autoReconnect = autoReconnect,
            ),
            listener = recorder,
            transports = wire,
        )
        return Triple(connection, wire, recorder)
    }

    @Test
    fun `hello is the first frame on the wire, and nothing precedes it (§2)`() {
        val (connection, wire, _) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            assertTrue(wire.sent.isEmpty(), "not a byte before the socket is open")

            wire.open()
            wire.waitForSent(1)
            assertEquals("""{"type":"hello","token":"t","proto":1,"sdk":"kotlin/0.1.0"}""", wire.sent[0])
        }
    }

    @Test
    fun `helloAck moves the FSM to CONNECTED and starts pings (§2, §5)`() {
        val (connection, wire, recorder) = fixture(pingIntervalMs = 60_000)
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.waitForSent(1)
            wire.deliver(helloAck(epoch = 5, rev = 184223))
            connection.awaitState(ClientState.CONNECTED)

            waitUntil("first ping") { wire.sent.size >= 2 }
            assertTrue(wire.sent[1].startsWith("""{"type":"ping","t":"""), "§5: the first ping follows helloAck")
            assertEquals(5, connection.epoch)
            assertEquals("s_fake", connection.session)
            waitUntil("both state callbacks") { recorder.states.size == 2 }
            assertEquals(listOf(ClientState.CONNECTING, ClientState.CONNECTED), recorder.states)
            assertEquals(false, recorder.acks.single().second, "first helloAck: nothing to compare against")
        }
    }

    @Test
    fun `an epoch change is reported on the helloAck that carries it (§2 v1_5)`() {
        val (connection, wire, recorder) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck(epoch = 1))
            connection.awaitState(ClientState.CONNECTED)

            wire.drop() // the wire dies; the FSM backs off and comes back
            waitUntil("reconnect") { wire.connects.get() == 2 }
            wire.open()
            wire.deliver(helloAck(epoch = 2)) // ...to a shard that was restored under us
            waitUntil("second helloAck") { recorder.acks.size == 2 }

            assertEquals(listOf(false, true), recorder.acks.map { it.second })
            assertEquals(2, connection.epoch)
        }
    }

    @Test
    fun `the same epoch after a reconnect is not a generation change`() {
        val (connection, wire, recorder) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck(epoch = 7))
            connection.awaitState(ClientState.CONNECTED)
            wire.drop()
            waitUntil("reconnect") { wire.connects.get() == 2 }
            wire.open()
            wire.deliver(helloAck(epoch = 7))
            waitUntil("second helloAck") { recorder.acks.size == 2 }
            assertEquals(listOf(false, false), recorder.acks.map { it.second })
        }
    }

    @Test
    fun `no pong within the timeout closes the socket and backs off (§5, §6)`() {
        val (connection, wire, recorder) = fixture(pingIntervalMs = 60_000, pongTimeoutMs = 50)
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            waitUntil("pong timeout") { recorder.pongTimeouts == 1 }
            waitUntil("retry") { wire.connects.get() == 2 }
            waitUntil("every state callback") { recorder.states.size == 4 }
            assertEquals(
                listOf(ClientState.CONNECTING, ClientState.CONNECTED, ClientState.WAITING, ClientState.CONNECTING),
                recorder.states,
            )
            assertTrue(wire.closes.isNotEmpty(), "the FSM closes the socket it stopped trusting")
        }
    }

    @Test
    fun `a pong cancels the timeout, and the connection stays up (§5)`() {
        val (connection, wire, recorder) = fixture(pingIntervalMs = 60_000, pongTimeoutMs = 300)
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)
            wire.waitForSent(2)

            wire.deliver("""{"type":"pong","t":1}""")
            Thread.sleep(400) // longer than the pong timeout that was outstanding
            assertEquals(0, recorder.pongTimeouts)
            assertEquals(ClientState.CONNECTED, connection.state.value)
        }
    }

    /** A 60ms hello timeout, and the two §5 timers pushed out of the way so only it can fire. */
    private fun helloTimeoutFixture() = fixture(
        limits = Limits(backoffCapMs = 20, backoffResetMs = 30_000, helloTimeoutMs = 60),
        pingIntervalMs = 60_000,
        pongTimeoutMs = 60_000,
    )

    @Test
    fun `a helloAck missing a field the SDK never reads surfaces instead of hanging (§2)`() {
        val (connection, wire, recorder) = helloTimeoutFixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.waitForSent(1)
            // `region` is REQUIRED to parse (Frames.kt) and is read by NOTHING. kotlinx throws
            // MissingFieldException, parseServerFrame swallows it as an unreadable frame, and
            // before this timer existed that left the FSM in CONNECTING forever — no state change,
            // no callback, no log, and a ready() that never returned. This is F1's own repro.
            wire.deliver("""{"type":"helloAck","rev":0,"epoch":1,"session":"s_fake"}""")
            Thread.sleep(20)
            assertTrue(recorder.acks.isEmpty(), "the malformed frame must not reach handleHelloAck")
            assertEquals(ClientState.CONNECTING, connection.state.value, "...and so nothing has moved yet")

            waitUntil("the hello timeout") { recorder.helloTimeouts == 1 }
            recorder.awaitState(ClientState.WAITING)
            waitUntil("a retry, onto whichever gateway answers next") { wire.connects.get() == 2 }
            assertTrue(recorder.authFailures.isEmpty(), "a timeout is not an auth rejection")
        }
    }

    @Test
    fun `a socket that opens and then says nothing is treated as a dead connection (§2, §6)`() {
        val (connection, wire, recorder) = helloTimeoutFixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.waitForSent(1) // hello went out; not one frame ever comes back

            waitUntil("the hello timeout") { recorder.helloTimeouts == 1 }
            waitUntil("every state callback") { recorder.states.size == 3 }
            assertEquals(
                listOf(ClientState.CONNECTING, ClientState.WAITING, ClientState.CONNECTING),
                recorder.states,
                "§6: a dead connection, down the path every other drop already takes",
            )
            assertTrue(wire.closes.isNotEmpty(), "the FSM closes the socket it gave up on")
            assertTrue(recorder.retries.isNotEmpty(), "and backs off like any other drop")
        }
    }

    @Test
    fun `a readable helloAck disarms the hello timer for the life of the connection (§2)`() {
        val (connection, wire, recorder) = helloTimeoutFixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            // 600ms held against a 60ms timeout — TEN full timer periods. A disarm test that runs
            // for less than the timeout it is disarming proves nothing at all.
            Thread.sleep(600)
            assertEquals(0, recorder.helloTimeouts, "disarmed by the helloAck, not merely not-yet-fired")
            assertEquals(ClientState.CONNECTED, connection.state.value)
            assertEquals(1, wire.connects.get(), "a healthy connection is never re-opened")
            assertTrue(wire.closes.isEmpty(), "nor closed")
        }
    }

    @Test
    fun `a 4401 while the hello timer is armed stays terminal (§6 v1_2)`() {
        val (connection, wire, recorder) = helloTimeoutFixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.waitForSent(1)
            // The 4401 lands BEFORE any helloAck — i.e. squarely inside the window this change
            // added a timer to. If that timer outlived the AUTH teardown it would retry a token
            // the server has already rejected, which is the one thing §6 v1.2 forbids.
            wire.drop(code = Close.AUTH, reason = "AUTH")
            connection.awaitState(ClientState.CLOSED)

            Thread.sleep(400) // ~7 hello-timeout periods, and many 20ms-capped backoffs
            assertEquals(listOf(Close.AUTH), recorder.authFailures)
            assertEquals(0, recorder.helloTimeouts, "the AUTH teardown cancelled the hello timer")
            assertEquals(1, wire.connects.get(), "§6 v1.2: only connect(newToken) leaves a 4401")
            assertEquals(ClientState.CLOSED, connection.state.value)
        }
    }

    @Test
    fun `a 4401 close stops the FSM — no retry with the same token (§6 v1_2)`() {
        val (connection, wire, recorder) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver("""{"type":"err","code":"AUTH","msg":"token expired"}""")
            wire.drop(code = Close.AUTH, reason = "AUTH")

            connection.awaitState(ClientState.CLOSED)
            waitUntil("the authFailure callback") { recorder.authFailures.isNotEmpty() }
            assertEquals(listOf(Close.AUTH), recorder.authFailures)
            Thread.sleep(150) // several 20ms-capped backoffs, had any been scheduled
            assertEquals(1, wire.connects.get(), "a dead token must not be retried")
            assertTrue(recorder.retries.isEmpty())

            // ...but a fresh token gets back in.
            connection.connect(token = "fresh")
            waitUntil("reconnect with a new token") { wire.connects.get() == 2 }
            wire.open()
            wire.waitForSent(2)
            assertTrue(wire.sent.last().contains(""""token":"fresh""""))
        }
    }

    @Test
    fun `autoReconnect off stops at CLOSED`() {
        val (connection, wire, _) = fixture(autoReconnect = false)
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)
            wire.drop()
            connection.awaitState(ClientState.CLOSED)
            Thread.sleep(100)
            assertEquals(1, wire.connects.get())
        }
    }

    @Test
    fun `unknown frames are ignored and a batch is unwrapped in order (§Transport, §3)`() {
        val (connection, wire, recorder) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            wire.deliver("""{"type":"getAck","reqId":1,"value":1,"rev":2}""") // §11, not implemented
            wire.deliver("garbage")
            wire.deliver(
                """{"type":"batch","frames":[
                    {"type":"delta","rev":1,"path":"p","op":"put","value":1},
                    {"type":"fromTheFuture"},
                    {"type":"ack","writeId":"w","rev":2}
                ]}""",
            )
            waitUntil("batch delivered") { recorder.frames.size >= 3 }
            Thread.sleep(50)

            assertEquals(
                listOf("helloAck", "delta", "ack"),
                recorder.frames.map { it::class.simpleName?.replaceFirstChar { c -> c.lowercase() } },
                "the batch's inner frames arrive individually, in array order, unknowns dropped alone",
            )
        }
    }

    @Test
    fun `connect() while WAITING cancels the scheduled retry — no second socket (§6)`() {
        val wire = FakeTransport()
        val recorder = RecordingListener()
        // A 270ms retry is still pending when connect() arrives; without the cancel in open() it
        // fires afterwards and opens a socket on top of the live one.
        val connection = Connection(
            ConnectionOptions(url = "ws://fake", token = "t", limits = Limits(backoffCapMs = 300), pingIntervalMs = 60_000),
            listener = recorder,
            transports = wire,
            random = FixedRandom(0.9),
        )
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            wire.drop()
            recorder.awaitState(ClientState.WAITING)
            connection.connect() // the app comes back before the backoff expires
            waitUntil("an immediate reconnect") { wire.connects.get() == 2 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            Thread.sleep(400) // well past the 270ms retry that was scheduled
            assertEquals(2, wire.connects.get(), "the cancelled retry must not open a second socket")
            assertEquals(ClientState.CONNECTED, connection.state.value)
        }
    }

    @Test
    fun `a second helloAck on one connection is ignored (§2)`() {
        val (connection, wire, recorder) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck(epoch = 1))
            connection.awaitState(ClientState.CONNECTED)
            waitUntil("the first helloAck callback") { recorder.acks.size == 1 }

            wire.deliver(helloAck(epoch = 9)) // a server bug: a second ack on a resumed connection
            Thread.sleep(100)

            assertEquals(1, recorder.acks.size, "the resume order must not run twice")
            assertEquals(1, connection.epoch, "and a stray ack cannot rewrite the generation")
        }
    }

    @Test
    fun `ready() fails fast once the FSM has stopped, instead of hanging (§6 v1_2)`() {
        val (connection, wire, _) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            // Connect for real first, so a helloAck IS on record: ready() must still refuse once the
            // FSM stops, rather than handing back a stale ack or hanging.
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)
            wire.drop(code = Close.AUTH, reason = "AUTH")
            connection.awaitState(ClientState.CLOSED)

            assertFailsWith<IllegalStateException> {
                runBlocking { withTimeout(2_000) { connection.ready() } }
            }
        }
    }

    @Test
    fun `the attempt counter resets only after the connection has been stable (§6)`() {
        val wire = FakeTransport()
        val recorder = RecordingListener()
        // Half of the window, so each attempt's delay is exactly half its ceiling.
        val connection = Connection(
            ConnectionOptions(
                url = "ws://fake",
                token = "t",
                limits = Limits(backoffResetMs = 100),
                pingIntervalMs = 60_000,
            ),
            listener = recorder,
            transports = wire,
            random = FixedRandom(0.5),
        )
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            wire.drop() // dropped immediately: attempt 0 -> ceiling 1s
            waitUntil("first retry") { recorder.retries.size == 1 }
            waitUntil("reconnect") { wire.connects.get() == 2 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            Thread.sleep(200) // ...held past backoffResetMs this time
            wire.drop()
            waitUntil("second retry") { recorder.retries.size == 2 }
            assertEquals(
                listOf(500L, 500L),
                recorder.retries,
                "a stable connection resets the counter; without it the second delay would be 1000",
            )
        }
    }

    @Test
    fun `§6 full jitter - the delay is uniform from zero up to min of 30s and 1s times 2 to the attempt`() {
        val limits = Limits()
        for (attempt in 0 until 10) assertEquals(0, backoffDelay(attempt, limits, 0.0))
        assertEquals(999, backoffDelay(0, limits, 0.999999))
        assertEquals(1999, backoffDelay(1, limits, 0.999999))
        assertEquals(3999, backoffDelay(2, limits, 0.999999))
        assertEquals(15999, backoffDelay(4, limits, 0.999999))
    }

    @Test
    fun `§6 backoff is capped at 30s however many attempts have failed`() {
        val limits = Limits()
        assertEquals(29999, backoffDelay(5, limits, 0.999999)) // 32s would exceed the cap
        assertEquals(29999, backoffDelay(20, limits, 0.999999))
        assertEquals(29999, backoffDelay(100, limits, 0.999999), "2^100 must not overflow into something un-capped")
        assertEquals(24, backoffDelay(10, Limits(backoffCapMs = 50), 0.499999), "the cap is configurable for tests")
    }
    // ---------------------------------------------------------------- WP3 seam: retryNow (§6)

    @Test
    fun `retryNow() skips the rest of a scheduled backoff and opens immediately (§6)`() {
        val wire = FakeTransport()
        val recorder = RecordingListener()
        // §6's first-attempt window is random(0, 1s) whatever the cap, so 0.999 is the longest wait
        // this seam can be shown skipping: 999ms scheduled, and the reconnect below happens in
        // single-digit ms. Without retryNow the elapsed assert is the one that fails.
        val connection = Connection(
            ConnectionOptions(url = "ws://fake", token = "t", pingIntervalMs = 60_000),
            listener = recorder,
            transports = wire,
            random = FixedRandom(0.999),
        )
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            wire.drop()
            recorder.awaitState(ClientState.WAITING)
            waitUntil("the backoff to be scheduled") { recorder.retries.size == 1 }
            assertEquals(999L, recorder.retries.single(), "the wait being skipped is a real one")

            val started = System.currentTimeMillis()
            connection.retryNow()
            waitUntil("an immediate reconnect", timeoutMs = 2_000) { wire.connects.get() == 2 }
            val elapsed = System.currentTimeMillis() - started
            assertTrue(elapsed < 500, "reconnected in ${elapsed}ms — the 999ms backoff was skipped")
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            Thread.sleep(400) // past the moment the cancelled retry would have fired
            assertEquals(2, wire.connects.get(), "the cancelled retry must not open a second socket")
        }
    }

    @Test
    fun `retryNow() does not revive a 4401-CLOSED connection (§6 v1_2)`() {
        val (connection, wire, _) = fixture()
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.drop(code = Close.AUTH, reason = "AUTH")
            connection.awaitState(ClientState.CLOSED)

            connection.retryNow() // the radio came back; the token is still dead
            Thread.sleep(150)
            assertEquals(1, wire.connects.get(), "only connect(newToken) leaves CLOSED")
            assertEquals(ClientState.CLOSED, connection.state.value)
        }
    }

    @Test
    fun `retryNow() while CONNECTED is a no-op — no second socket`() {
        // A real pong timeout, so the sleep below cannot mistake a §5 reconnect for retryNow's work.
        val (connection, wire, _) = fixture(pongTimeoutMs = 60_000)
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            connection.retryNow()
            Thread.sleep(150)
            assertEquals(1, wire.connects.get())
            assertEquals(ClientState.CONNECTED, connection.state.value)
        }
    }

    // ------------------------------------------------------- WP3 seam: setBackgrounded (§5)

    @Test
    fun `setBackgrounded() switches the ping cadence, from the next ping (§5)`() {
        val wire = FakeTransport()
        // 30ms "foreground", 10s "background" — the §5 25s/60s pair, scaled so a test can see it.
        val connection = Connection(
            ConnectionOptions(
                url = "ws://fake",
                token = "t",
                limits = Limits(pingBackgroundMs = 10_000),
                pingIntervalMs = 30,
                pongTimeoutMs = 60_000,
            ),
            transports = wire,
        )
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)

            waitUntil("the foreground cadence to fire repeatedly") { wire.sentOf("ping").size >= 4 }
            assertEquals(false, connection.backgrounded)

            connection.setBackgrounded(true)
            waitUntil("the switch to reach the state dispatcher") { connection.backgrounded }
            Thread.sleep(100) // long enough for the in-flight 30ms delay to expire and one more ping
            val settled = wire.sentOf("ping").size
            Thread.sleep(300) // ...at 30ms that would be ten more pings
            assertEquals(settled, wire.sentOf("ping").size, "backgrounded, the next ping is 10s away")

            connection.setBackgrounded(false)
            waitUntil("the switch back") { !connection.backgrounded }
        }
    }
    @Test
    fun `coming back to the foreground probes the connection immediately (Gate B ruling Q3)`() {
        val wire = FakeTransport()
        val connection = Connection(
            // A 60s foreground interval: after helloAck's first ping, nothing pings on its own.
            ConnectionOptions(
                url = "ws://fake",
                token = "t",
                limits = Limits(pingBackgroundMs = 60_000),
                pingIntervalMs = 60_000,
                pongTimeoutMs = 60_000,
            ),
            transports = wire,
        )
        connection.use {
            connection.connect()
            waitUntil("connecting") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(helloAck())
            connection.awaitState(ClientState.CONNECTED)
            waitUntil("§5's first ping") { wire.sentOf("ping").size == 1 }

            // Not a transition: already in the foreground, so nothing is probed.
            connection.setBackgrounded(false)
            Thread.sleep(100)
            assertEquals(1, wire.sentOf("ping").size, "a repeated foreground must not fire a ping")

            connection.setBackgrounded(true)
            waitUntil("the background cadence") { connection.backgrounded }
            Thread.sleep(100)
            assertEquals(1, wire.sentOf("ping").size, "going away costs nothing")

            connection.setBackgrounded(false)
            waitUntil("the foreground probe") { wire.sentOf("ping").size == 2 }
            assertEquals(ClientState.CONNECTED, connection.state.value)
        }
    }
}
