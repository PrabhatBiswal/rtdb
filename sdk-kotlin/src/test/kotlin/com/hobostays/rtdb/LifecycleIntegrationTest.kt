package com.hobostays.rtdb

import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.Close
import com.hobostays.rtdb.core.Connection
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Err
import com.hobostays.rtdb.core.Limits
import com.hobostays.rtdb.core.Pong
import java.io.File
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

/**
 * WORKLOAD §6 Gate B: §2/§5/§6 against the REAL WP1 gateway, spawned as a child process. No mock
 * servers — the only fault injection is the network (see [Proxy]) or killing the process.
 */
class LifecycleIntegrationTest {

    private val fastBackoff = Limits(backoffCapMs = 40, backoffResetMs = 30_000)

    private fun connection(
        url: String,
        token: String = devToken(),
        limits: Limits = fastBackoff,
        listener: RecordingListener = RecordingListener(),
        pingIntervalMs: Long = 60_000,
        pongTimeoutMs: Long = 10_000,
        autoReconnect: Boolean = true,
    ) = Connection(
        ConnectionOptions(
            url = url,
            token = token,
            sdk = "kotlin/0.1.0",
            limits = limits,
            pingIntervalMs = pingIntervalMs,
            pongTimeoutMs = pongTimeoutMs,
            autoReconnect = autoReconnect,
        ),
        listener = listener,
    )

    @Test
    fun `happy connect - hello is answered with helloAck carrying rev, epoch, region and session`() {
        val gateway = GatewayProcess.start()
        try {
            val recorder = RecordingListener()
            connection(gateway.url, listener = recorder).use { connection ->
                connection.connect()
                val ack = runBlocking { withTimeout(10_000) { connection.ready() } }

                assertEquals(0, ack.rev, "a fresh shard's head")
                assertTrue(ack.epoch >= 1, "§2 v1.5: helloAck carries the shard epoch, got ${ack.epoch}")
                assertEquals("ap-south-1", ack.region)
                assertTrue(Regex("^s_[0-9a-f]{8}$").matches(ack.session), "session was ${ack.session}")
                assertEquals(ClientState.CONNECTED, connection.state.value)
                assertEquals(ack.epoch, connection.epoch)
                waitUntil("the helloAck callback") { recorder.acks.isNotEmpty() }
                assertEquals(false, recorder.acks.single().second, "nothing to compare a first epoch against")
            }
        } finally {
            gateway.stop()
        }
    }

    @Test
    fun `the gateway answers pings with a pong that echoes t verbatim (§5)`() {
        val gateway = GatewayProcess.start()
        try {
            val recorder = RecordingListener()
            connection(gateway.url, listener = recorder, pingIntervalMs = 200).use { connection ->
                connection.connect()
                runBlocking { withTimeout(10_000) { connection.ready() } }
                waitUntil("a pong") { recorder.frames.any { it is Pong } }
                Thread.sleep(500) // several ping intervals: a pong keeps the connection alive
                assertEquals(0, recorder.pongTimeouts)
                assertEquals(ClientState.CONNECTED, connection.state.value)
            }
        } finally {
            gateway.stop()
        }
    }

    @Test
    fun `a bad token gets err AUTH and close 4401, and is never retried (§2, §6 v1_2)`() {
        val gateway = GatewayProcess.start()
        try {
            val recorder = RecordingListener()
            connection(gateway.url, token = devToken(secret = "wrong-secret"), listener = recorder)
                .use { connection ->
                    connection.connect()
                    connection.awaitState(ClientState.CLOSED, timeoutMs = 10_000)
                    waitUntil("the authFailure callback") { recorder.authFailures.isNotEmpty() }

                    assertEquals(listOf(Close.AUTH), recorder.authFailures)
                    val err = recorder.frames.filterIsInstance<Err>().single()
                    assertEquals("AUTH", err.code)
                    Thread.sleep(200) // many 40ms-capped backoffs, had any been scheduled
                    assertTrue(recorder.retries.isEmpty(), "a dead token must not be retried")

                    // ...but a fresh token gets back in.
                    connection.connect(token = devToken())
                    val ack = runBlocking { withTimeout(10_000) { connection.ready() } }
                    assertTrue(ack.epoch >= 1)
                }
        } finally {
            gateway.stop()
        }
    }

    @Test
    fun `no pong within the timeout drops the connection and the FSM backs off (§5, §6)`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                val recorder = RecordingListener()
                // Pings keep coming while the pongs cannot get back — the first ping after the
                // pause is the one whose answer never arrives.
                connection(proxy.url, listener = recorder, pingIntervalMs = 100, pongTimeoutMs = 300)
                    .use { connection ->
                        connection.connect()
                        runBlocking { withTimeout(10_000) { connection.ready() } }

                        // The gateway still answers; its pongs just cannot reach us any more.
                        proxy.pauseDownstream()
                        waitUntil("pong timeout", timeoutMs = 10_000) { recorder.pongTimeouts >= 1 }
                        recorder.awaitState(ClientState.WAITING)
                        assertTrue(recorder.retries.isNotEmpty(), "§6: a drop schedules a backoff")

                        // ...and once the wire works again it really does come back up.
                        proxy.resumeDownstream()
                        runBlocking { withTimeout(15_000) { connection.ready() } }
                        assertEquals(ClientState.CONNECTED, connection.state.value)
                    }
            } finally {
                gateway.stop()
            }
        }
    }

    @Test
    fun `a cut wire reconnects on its own, without being told to (§6)`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                val recorder = RecordingListener()
                connection(proxy.url, listener = recorder).use { connection ->
                    connection.connect()
                    val first = runBlocking { withTimeout(10_000) { connection.ready() } }

                    proxy.cut()
                    recorder.awaitState(ClientState.WAITING, timeoutMs = 10_000)
                    val second = runBlocking { withTimeout(15_000) { connection.ready() } }

                    assertNotEquals(first.session, second.session, "a reconnect is a new session")
                    assertEquals(first.epoch, second.epoch, "the shard did not change generation")
                }
            } finally {
                gateway.stop()
            }
        }
    }

    @Test
    fun `a gateway restart WITH persistence keeps the epoch, so nothing is dropped (§2 v1_5)`() {
        val directory = Files.createTempDirectory("rtdb-kotlin-").toFile()
        val gateway = GatewayProcess.start(persist = File(directory, "oplog.jsonl"))
        try {
            val recorder = RecordingListener()
            connection(gateway.url, listener = recorder).use { connection ->
                connection.connect()
                val first = runBlocking { withTimeout(10_000) { connection.ready() } }

                gateway.restart() // SIGKILL, then back on the same port with its oplog
                recorder.awaitState(ClientState.WAITING, timeoutMs = 15_000)
                waitUntil("a second helloAck", timeoutMs = 20_000) { recorder.acks.size == 2 }

                assertEquals(first.epoch, recorder.acks[1].first.epoch, "same data, same generation")
                assertEquals(false, recorder.acks[1].second, "nothing to drop")
            }
        } finally {
            gateway.stop()
            directory.deleteRecursively()
        }
    }

    @Test
    fun `a gateway restart WITHOUT persistence changes the epoch and orders a wholesale drop (§2 v1_5)`() {
        val gateway = GatewayProcess.start()
        try {
            val recorder = RecordingListener()
            connection(gateway.url, listener = recorder).use { connection ->
                connection.connect()
                val first = runBlocking { withTimeout(10_000) { connection.ready() } }

                gateway.restart() // head back to 0: every rev this client holds is from a dead generation
                recorder.awaitState(ClientState.WAITING, timeoutMs = 15_000)
                waitUntil("a second helloAck", timeoutMs = 20_000) { recorder.acks.size == 2 }

                val (ack, epochChanged) = recorder.acks[1]
                assertNotEquals(first.epoch, ack.epoch, "a broken rev promise means a new epoch")
                assertTrue(epochChanged, "§2 v1.5: the owner must drop mirrors, revs, tombstones and lastRevs")
                assertEquals(ack.epoch, connection.epoch, "and the new epoch is stored")
            }
        } finally {
            gateway.stop()
        }
    }
}
