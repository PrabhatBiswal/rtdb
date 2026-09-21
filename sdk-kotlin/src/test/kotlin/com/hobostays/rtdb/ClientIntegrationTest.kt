package com.hobostays.rtdb

import com.hobostays.rtdb.api.ChildEvent
import com.hobostays.rtdb.api.INFO_CONNECTED
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.WriteResult
import com.hobostays.rtdb.api.childEvents
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Limits
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
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

    /** The same story as the callback test above, told through the Flow (§5.32). */
    @Test
    fun `childEvents() delivers add, change and remove in order (§5,32)`() {
        val gateway = GatewayProcess.start()
        try {
            client(gateway.url).use { writer ->
                client(gateway.url).use { reader ->
                    val seen = CopyOnWriteArrayList<String>()
                    val collector = CoroutineScope(Dispatchers.Default).launch {
                        reader.ref("room").childEvents().collect { seen += it.label() }
                    }
                    // Collecting is what subscribes; the sub appearing is the flow's listener landing.
                    waitUntil("the flow to subscribe to \"room\"") { reader.subscriptionPaths.contains("room") }

                    writer.ref("room/a").setValue(JsonPrimitive(1))
                    waitUntil("child added") { seen.size == 1 }
                    writer.ref("room/a").setValue(JsonPrimitive(2))
                    waitUntil("child changed") { seen.size == 2 }
                    writer.ref("room/a").removeValue()
                    waitUntil("child removed") { seen.size == 3 }

                    assertEquals(listOf("added:a=1", "changed:a=2", "removed:a=2"), seen.toList())
                    assertConverged(gateway.url, writer, reader)

                    // Cancelling collection unlistens: the flow held the only listener on "room", so
                    // the subscription itself goes away (RtdbClient.removeListener).
                    collector.cancel()
                    waitUntil("the subscription to be dropped") { !reader.subscriptionPaths.contains("room") }
                }
            }
        } finally {
            gateway.stop()
        }
    }

    /**
     * THE TOOTH (§5.32): a collector that arrives after 100 children already exist is replayed all
     * 100 in one tight loop on the client dispatcher (RtdbClient.kt:180). With callbackFlow's
     * default 64-slot buffer `trySend` refuses past 64 and drops silently; this asserts the count.
     */
    @Test
    fun `childEvents() loses nothing replaying 100 existing children to a late collector (§5,32)`() {
        val gateway = GatewayProcess.start()
        try {
            client(gateway.url).use { writer ->
                client(gateway.url).use { reader ->
                    val children = (1..100).associate { "c$it" to JsonPrimitive(it) as JsonElement }
                    writer.ref("room").setValue(JsonObject(children))

                    // The reader holds the full subtree BEFORE it collects: this is the late-listener
                    // replay path, not the fresh-snapshot one.
                    val values = ValueRecorder()
                    reader.ref("room").addValueEventListener(values)
                    waitUntil("the reader to mirror all 100 children") {
                        ((values.last as? JsonObject)?.size ?: 0) == 100
                    }

                    val seen = CopyOnWriteArrayList<ChildEvent>()
                    val collector = CoroutineScope(Dispatchers.Default).launch {
                        reader.ref("room").childEvents().collect {
                            seen += it
                            // A collector that DOES something per child — an Android list update is
                            // milliseconds, the replay loop is microseconds. Without it the consumer
                            // keeps up with all 100 and the buffer is never under pressure at all.
                            delay(2)
                        }
                    }
                    // Not waitUntil's own check: a timeout here must report the COUNT, which is the
                    // whole finding, and waitUntil's label is built before the wait starts.
                    runCatching { waitUntil("100 replayed child_added events", 10_000) { seen.size >= 100 } }
                    collector.cancel()

                    assertEquals(100, seen.size, "replayed events")
                    assertTrue(seen.all { it is ChildEvent.Added }, "every replayed event is an Added")
                    assertEquals(
                        (1..100).map { "c$it" }.toSet(),
                        seen.mapNotNull { it.snapshot.key }.toSet(),
                        "every child key arrived exactly once",
                    )
                }
            }
        } finally {
            gateway.stop()
        }
    }

    /**
     * The child baseline is per-SUBSCRIPTION, not per-listener, and a value listener can keep a sub
     * alive after a collector is gone (RtdbClient.kt:182). A second collector must therefore be
     * replayed the tree as it is NOW — re-baselining on every add is what makes that true, and
     * dropping it replays the children the first collector saw.
     */
    @Test
    fun `a second childEvents() collector replays the tree as it is now (§5,32)`() {
        val gateway = GatewayProcess.start()
        try {
            client(gateway.url).use { writer ->
                client(gateway.url).use { reader ->
                    // Outlives both collectors, so the subscription is never dropped between them.
                    val values = ValueRecorder()
                    reader.ref("room").addValueEventListener(values)

                    writer.ref("room/a").setValue(JsonPrimitive(1))
                    values.awaitValue(json("""{"a":1}"""))

                    val first = CopyOnWriteArrayList<String>()
                    val c1 = CoroutineScope(Dispatchers.Default).launch {
                        reader.ref("room").childEvents().collect { first += it.label() }
                    }
                    waitUntil("the first collector to be replayed a") { first.size == 1 }
                    c1.cancel()
                    // removeListener is posted to the client dispatcher, so the unlisten is not
                    // synchronous with cancel(). No new probe for it: if it never landed, `first`
                    // grows past one event and the assertion at the end of this test says so.
                    Thread.sleep(200)

                    // The tree changes while NO child listener exists — the window where a frozen
                    // baseline goes stale.
                    writer.ref("room/a").removeValue()
                    writer.ref("room/b").setValue(JsonPrimitive(2))
                    values.awaitValue(json("""{"b":2}"""))

                    val second = CopyOnWriteArrayList<String>()
                    val c2 = CoroutineScope(Dispatchers.Default).launch {
                        reader.ref("room").childEvents().collect { second += it.label() }
                    }
                    waitUntil("the second collector to be replayed b") { second.size >= 1 }
                    // A stale baseline shows up as an EXTRA event after the first, so give it room.
                    Thread.sleep(300)
                    c2.cancel()

                    assertEquals(listOf("added:b=2"), second.toList(), "second collector's replay")
                    assertEquals(listOf("added:a=1"), first.toList(), "first collector's replay")
                    assertConverged(gateway.url, writer, reader)
                }
            }
        } finally {
            gateway.stop()
        }
    }
}
