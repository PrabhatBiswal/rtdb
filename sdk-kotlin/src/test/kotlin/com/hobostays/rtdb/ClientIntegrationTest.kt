package com.hobostays.rtdb

import com.hobostays.rtdb.api.INFO_CONNECTED
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.WriteResult
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Limits
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * WORKLOAD §6 Gate C: §3/§4/§7 against the REAL WP1 gateway. Every test ends the same way as WP1's
 * chaos suite does — every client's serverState equals the server's own view of the paths it
 * subscribes to, with nothing left pending.
 */
class ClientIntegrationTest {

    private val limits = Limits(backoffCapMs = 40)

    private fun client(url: String): RtdbClient =
        RtdbClient(
            ConnectionOptions(
                url = url,
                token = devToken(),
                sdk = "kotlin/0.1.0",
                limits = limits,
                pingIntervalMs = 60_000,
            ),
        ).also {
            it.connect()
            runBlocking { withTimeout(15_000) { it.ready() } }
        }

    /**
     * The server's own view of a path, read over the wire on a throwaway connection — the only
     * honest reference when the gateway is a separate process (WP1's `serverValue` helper).
     */
    private fun serverValue(url: String, path: String): JsonElement {
        client(url).use { probe ->
            val values = ValueRecorder()
            probe.ref(path).addValueEventListener(values)
            waitUntil("the server's snapshot of \"$path\"") { values.values.isNotEmpty() }
            return values.last!!
        }
    }

    /** Gate D's standing assertion, used here from the start (WP1's `assertConverged`). */
    private fun assertConverged(url: String, vararg clients: RtdbClient) {
        for ((index, client) in clients.withIndex()) {
            // Convergence is eventual: a delta echo can beat its own ack across the wire, so the
            // queue drains a moment after the value is already right.
            waitUntil("client $index to settle every write") { client.pendingWriteIds.isEmpty() }
            for (path in client.subscriptionPaths) {
                val expected = serverValue(url, path)
                assertEquals(expected, client.mirror.serverValue(path), "client $index diverged at \"$path\"")
                assertEquals(expected, client.value(path), "client $index view != serverState at \"$path\"")
            }
        }
    }

    @Test
    fun `two clients converge through the gateway - put, merge and remove all propagate`() {
        val gateway = GatewayProcess.start()
        try {
            client(gateway.url).use { a ->
                client(gateway.url).use { b ->
                    val seenByB = ValueRecorder()
                    a.ref("room").addValueEventListener(ValueRecorder())
                    b.ref("room").addValueEventListener(seenByB)
                    seenByB.awaitValue(JsonNull)

                    val writes = WriteRecorder()
                    a.ref("room/player").setValue(json("""{"name":"Ravi","score":42}"""), writes.callback)
                    seenByB.awaitValue(json("""{"player":{"name":"Ravi","score":42}}"""))

                    // §4: a merge writes deep keys atomically, and a null child deletes.
                    b.ref("room/player").updateChildren(
                        mapOf("score" to JsonPrimitive(50), "stats/wins" to JsonPrimitive(3), "name" to JsonNull),
                        writes.callback,
                    )
                    seenByB.awaitValue(json("""{"player":{"score":50,"stats":{"wins":3}}}"""))

                    a.ref("room/player/stats").removeValue(writes.callback)
                    seenByB.awaitValue(json("""{"player":{"score":50}}"""))

                    writes.awaitCount(3)
                    assertTrue(writes.results.all { it is WriteResult.Committed }, "every write committed: ${writes.results}")
                    assertConverged(gateway.url, a, b)
                }
            }
        } finally {
            gateway.stop()
        }
    }

    @Test
    fun `child events fire from another client's writes (§7)`() {
        val gateway = GatewayProcess.start()
        try {
            client(gateway.url).use { writer ->
                client(gateway.url).use { reader ->
                    val children = ChildRecorder()
                    val values = ValueRecorder()
                    reader.ref("room").addValueEventListener(values)
                    reader.ref("room").addChildEventListener(children)
                    values.awaitValue(JsonNull)

                    writer.ref("room/a").setValue(JsonPrimitive(1))
                    waitUntil("child_added") { children.events == listOf("added:a=1") }
                    writer.ref("room/a").setValue(JsonPrimitive(2))
                    waitUntil("child_changed") { children.events.size == 2 }
                    writer.ref("room/a").removeValue()
                    waitUntil("child_removed") { children.events.size == 3 }

                    assertEquals(listOf("added:a=1", "changed:a=2", "removed:a=2"), children.events)
                    assertConverged(gateway.url, writer, reader)
                }
            }
        } finally {
            gateway.stop()
        }
    }

    @Test
    fun `CAS commits once and rejects the stale racer with fresh state (§4)`() {
        val gateway = GatewayProcess.start()
        try {
            client(gateway.url).use { a ->
                client(gateway.url).use { b ->
                    val seed = WriteRecorder()
                    a.ref("p/score").setValue(JsonPrimitive(0), seed.callback)
                    seed.awaitCount(1)
                    val base = (seed.results.single() as WriteResult.Committed).rev

                    val first = WriteRecorder()
                    val second = WriteRecorder()
                    a.ref("p/score").compareAndSet(base, JsonPrimitive(1), first.callback)
                    first.awaitCount(1)
                    b.ref("p/score").compareAndSet(base, JsonPrimitive(2), second.callback)
                    second.awaitCount(1)

                    val winner = first.results.single()
                    assertTrue(winner is WriteResult.Committed, "the first CAS commits: $winner")
                    val loser = second.results.single()
                    assertTrue(loser is WriteResult.Rejected, "a stale expectedRev is rejected: $loser")
                    assertEquals(JsonPrimitive(1), (loser as WriteResult.Rejected).value, "carrying the state that beat it")
                    assertEquals((winner as WriteResult.Committed).rev, loser.rev)
                }
            }
        } finally {
            gateway.stop()
        }
    }

    @Test
    fun `a reconnect resumes from the stored lastRev and catches up with deltas (§3, §6)`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                client(gateway.url).use { writer ->
                    client(proxy.url).use { reader ->
                        val values = ValueRecorder()
                        writer.ref("room/seed").setValue(JsonPrimitive("v"))
                        reader.ref("room").addValueEventListener(values)
                        values.awaitValue(json("""{"seed":"v"}"""))

                        proxy.cut() // the reader is away while three writes land
                        for (i in 0 until 3) writer.ref("room/m$i").setValue(JsonPrimitive(i))
                        waitUntil("the writes land") { serverValue(gateway.url, "room/m2") == JsonPrimitive(2) }

                        values.awaitValue(json("""{"seed":"v","m0":0,"m1":1,"m2":2}"""), timeoutMs = 15_000)
                        assertConverged(gateway.url, reader)
                    }
                }
            } finally {
                gateway.stop()
            }
        }
    }

    @Test
    fun `a write issued while disconnected replays on reconnect, under its original writeId (§6)`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                client(proxy.url).use { c ->
                    val values = ValueRecorder()
                    c.ref("room").addValueEventListener(values)
                    values.awaitValue(JsonNull)

                    proxy.cut()
                    val writes = WriteRecorder()
                    c.ref("room/queued").setValue(JsonPrimitive("later"), writes.callback)
                    // §7: the overlay shows it immediately, with no server anywhere in sight.
                    values.awaitValue(json("""{"queued":"later"}"""))
                    assertEquals(1, c.pendingWriteIds.size)

                    writes.awaitCount(1, timeoutMs = 20_000)
                    assertTrue(writes.results.single() is WriteResult.Committed)
                    assertConverged(gateway.url, c)
                }
            } finally {
                gateway.stop()
            }
        }
    }

    @Test
    fun `info connected follows the real connection (§7)`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                client(proxy.url).use { c ->
                    val info = ValueRecorder()
                    c.ref(INFO_CONNECTED).addValueEventListener(info)
                    info.awaitValue(JsonPrimitive(true))

                    proxy.cut()
                    info.awaitValue(JsonPrimitive(false))
                    info.awaitValue(JsonPrimitive(true), timeoutMs = 15_000)
                }
            } finally {
                gateway.stop()
            }
        }
    }
}
