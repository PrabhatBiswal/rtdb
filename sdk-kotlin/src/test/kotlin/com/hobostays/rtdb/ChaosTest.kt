package com.hobostays.rtdb

import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.WriteResult
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.Limits
import com.hobostays.rtdb.core.OverlayOp
import com.hobostays.rtdb.core.Put
import java.io.File
import java.nio.file.Files
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * WORKLOAD §6 Gate D — WP1's chaos scenarios that exercise the CLIENT contract, ported against the
 * real gateway process and the Kotlin TCP proxy. Every scenario ends the same way: every client
 * mirror equals the server's own state on every path it subscribes to, and nothing was applied twice.
 *
 * Faults are injected at the network ([Proxy]) or by killing the gateway process — never by a
 * test-only hook inside the server or the SDK. Where WP1 read the gateway's storage in-process, this
 * suite reads the same facts off the wire, because here the gateway is a separate OS process.
 *
 * S7 (subscribe/write race) is server-side and already proven in WP1; S8 (tombstone injection) is
 * covered by the ported mirror unit tests (WORKLOAD §6).
 */
class ChaosTest {

    private val fast = Limits(backoffCapMs = 40)

    private fun downFor(client: RtdbClient) =
        waitUntil("the client to notice the wire is gone", 20_000) {
            client.state.value != ClientState.CONNECTED
        }

    // ---------------------------------------------------------------- 1

    @Test
    fun `S1 lost ack - the ack dies after the commit, and the replay returns the ORIGINAL rev`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                rtdbClient(gateway.url, limits = fast).use { observer ->
                    rtdbClient(proxy.url, limits = fast).use { writer ->
                        val seen = ValueRecorder()
                        observer.ref("p").addValueEventListener(seen)
                        writer.ref("p").addValueEventListener(ValueRecorder())
                        seen.awaitValue(JsonNull)

                        proxy.pauseDownstream() // nothing the server sends can reach the writer
                        val writes = WriteRecorder()
                        writer.ref("p/score").setValue(JsonPrimitive(42), writes.callback)
                        seen.awaitValue(json("""{"score":42}"""), timeoutMs = 20_000) // it committed
                        val committedRev = observer.lastRev("p")

                        proxy.cut() // ...and now the connection dies, taking the ack with it
                        proxy.resumeDownstream()

                        writes.awaitCount(1, timeoutMs = 20_000)
                        val ack = writes.results.single()
                        assertTrue(ack is WriteResult.Committed, "the replay settled: $ack")
                        assertEquals(
                            committedRev,
                            (ack as WriteResult.Committed).rev,
                            "the replay is acked with the rev the FIRST commit got (§4 dedup)",
                        )
                        Thread.sleep(200)
                        assertEquals(committedRev, observer.lastRev("p"), "exactly-once: the replay burned no rev")
                        assertConverged(gateway.url, observer, writer)
                    }
                }
            } finally {
                gateway.stop()
            }
        }
    }

    // ---------------------------------------------------------------- 2

    @Test
    fun `S2 reconnect catch-up - a short gap is served from the oplog, with no second snapshot`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                rtdbClient(gateway.url, limits = fast).use { writer ->
                    val spy = SpyTransports()
                    rtdbClient(proxy.url, limits = fast, transports = spy).use { reader ->
                        val values = ValueRecorder()
                        writer.ref("room/seed").setValue(JsonPrimitive("v")) // a rev > 0 to resume from
                        reader.ref("room").addValueEventListener(values)
                        values.awaitValue(json("""{"seed":"v"}"""))

                        proxy.blackhole() // down, and staying down until every write has landed
                        downFor(reader)
                        for (i in 0 until 3) writer.ref("room/m$i").setValue(JsonPrimitive(i))
                        waitUntil("the writes to land") { serverValue(gateway.url, "room/m2") == JsonPrimitive(2) }
                        proxy.restore()

                        values.awaitValue(json("""{"seed":"v","m0":0,"m1":1,"m2":2}"""), timeoutMs = 20_000)
                        assertEquals(1, spy.countOf("snapshot"), "a retained lastRev is served with deltas only (§3)")
                        assertConverged(gateway.url, reader)
                    }
                }
            } finally {
                gateway.stop()
            }
        }
    }

    // ---------------------------------------------------------------- 3

    @Test
    fun `S3 snapshot fallback - too far behind and the server re-snapshots instead`() {
        val gateway = GatewayProcess.start(limitsJson = """{"CATCHUP_LIMIT":2}""")
        Proxy.start(gateway.port).use { proxy ->
            try {
                rtdbClient(gateway.url, limits = fast).use { writer ->
                    val spy = SpyTransports()
                    rtdbClient(proxy.url, limits = fast, transports = spy).use { reader ->
                        val values = ValueRecorder()
                        writer.ref("room/seed").setValue(JsonPrimitive("v"))
                        reader.ref("room").addValueEventListener(values)
                        values.awaitValue(json("""{"seed":"v"}"""))

                        proxy.blackhole()
                        downFor(reader)
                        for (i in 0 until 5) writer.ref("room/m$i").setValue(JsonPrimitive(i)) // > CATCHUP_LIMIT
                        waitUntil("the writes to land") { serverValue(gateway.url, "room/m4") == JsonPrimitive(4) }
                        proxy.restore()

                        values.awaitValue(
                            json("""{"seed":"v","m0":0,"m1":1,"m2":2,"m3":3,"m4":4}"""),
                            timeoutMs = 20_000,
                        )
                        assertEquals(2, spy.countOf("snapshot"), "too far behind to stream: a fresh snapshot (§3)")
                        assertConverged(gateway.url, reader)
                    }
                }
            } finally {
                gateway.stop()
            }
        }
    }

    // ---------------------------------------------------------------- 4

    @Test
    fun `S4 duplicate writeId storm - 5 sends across 2 connections, one commit, 5 identical acks`() {
        val gateway = GatewayProcess.start()
        try {
            val spyA = SpyTransports()
            val spyB = SpyTransports()
            rtdbClient(gateway.url, limits = fast, transports = spyA).use { a ->
                rtdbClient(gateway.url, limits = fast, transports = spyB).use { b ->
                    val observer = ValueRecorder()
                    a.ref("p").addValueEventListener(observer)
                    observer.awaitValue(JsonNull)

                    // §1: the server deduplicates on writeId, so the SAME write may be sent by
                    // anyone, any number of times, and must commit exactly once.
                    val writeId = "0d0e1f2a-3b4c-4d5e-8f90-a1b2c3d4e5f6"
                    val frame = Put(writeId = writeId, path = "p", value = JsonPrimitive("first"))
                    val overlay = OverlayOp("put", "p", JsonPrimitive("first"))
                    val writes = WriteRecorder()
                    a.write(frame, overlay, writes.callback)
                    b.write(frame, overlay, writes.callback)
                    a.resend(writeId)
                    b.resend(writeId)
                    a.resend(writeId)

                    waitUntil("five acks", 20_000) { spyA.countOf("ack") + spyB.countOf("ack") == 5 }
                    Thread.sleep(300)

                    val acks = spyA.framesOf("ack") + spyB.framesOf("ack")
                    assertEquals(5, acks.size, "one ack per send, however many sends there were")
                    assertEquals(
                        1,
                        acks.map { it["rev"]!!.jsonPrimitive.content }.distinct().size,
                        "every ack carries the SAME rev — one commit (§4 step 4)",
                    )
                    assertEquals(JsonPrimitive("first"), serverValue(gateway.url, "p"))
                    assertEquals(
                        acks.first()["rev"]!!.jsonPrimitive.content.toLong(),
                        a.lastRev("p"),
                        "the shard advanced by exactly that one commit",
                    )
                    assertConverged(gateway.url, a, b)
                }
            }
        } finally {
            gateway.stop()
        }
    }

    // ---------------------------------------------------------------- 5

    @Test
    fun `S5 concurrent CAS - five racers, exactly one commit and four rejections with fresh state`() {
        val gateway = GatewayProcess.start()
        try {
            val clients = List(5) { rtdbClient(gateway.url, limits = fast) }
            try {
                for (client in clients) client.ref("p").addValueEventListener(ValueRecorder())
                val seed = WriteRecorder()
                clients[0].ref("p/score").setValue(JsonPrimitive(0), seed.callback)
                seed.awaitCount(1)
                val base = (seed.results.single() as WriteResult.Committed).rev
                for (client in clients) {
                    waitUntil("everyone to see the seed") { client.value("p/score") == JsonPrimitive(0) }
                }

                // All five fire before any of them can have heard an answer.
                val race = WriteRecorder()
                for ((index, client) in clients.withIndex()) {
                    client.ref("p/score").compareAndSet(base, JsonPrimitive(index + 1), race.callback)
                }
                race.awaitCount(5, timeoutMs = 20_000)

                val committed = race.results.filterIsInstance<WriteResult.Committed>()
                val rejected = race.results.filterIsInstance<WriteResult.Rejected>()
                assertEquals(1, committed.size, "exactly one CAS may win: ${race.results}")
                assertEquals(4, rejected.size)
                val winner = serverValue(gateway.url, "p/score")
                for (loss in rejected) {
                    assertEquals(winner, loss.value, "a rejection carries the state that beat it")
                    assertEquals(committed.single().rev, loss.rev)
                }
                assertConverged(gateway.url, *clients.toTypedArray())
            } finally {
                clients.forEach { it.close() }
            }
        } finally {
            gateway.stop()
        }
    }

    // ---------------------------------------------------------------- 6

    @Test
    fun `S6 slow consumer - the queue overflows, the server sends resync, the mirror converges`() {
        val gateway = GatewayProcess.start(limitsJson = """{"SEND_QUEUE_MAX":1024,"DELTA_BATCH_MS":5}""")
        Proxy.start(gateway.port).use { proxy ->
            try {
                rtdbClient(gateway.url, limits = fast).use { writer ->
                    val spy = SpyTransports()
                    rtdbClient(proxy.url, limits = fast, transports = spy).use { reader ->
                        val values = ValueRecorder()
                        reader.ref("room").addValueEventListener(values)
                        values.awaitValue(JsonNull)

                        proxy.pauseDownstream() // the client stops reading; backpressure is real
                        // WP1's numbers: 2.6 MB, deliberately under SNAPSHOT_MAX (§9's 4 MiB), so the
                        // repair CAN be served as a snapshot. Past that ceiling §3 terminates the
                        // subscription with TOOBIG instead — see the Gate D report's finding 1.
                        val blob = "x".repeat(64 * 1024)
                        val flood = WriteRecorder()
                        for (i in 0 until 40) {
                            writer.ref("room/big$i").setValue(JsonPrimitive("$i:$blob"), flood.callback)
                        }
                        // Every write acked means every delta has been handed to the reader's sink,
                        // which is the only way the overflow is a fact rather than a race.
                        flood.awaitCount(40, timeoutMs = 60_000)
                        Thread.sleep(200)
                        proxy.resumeDownstream()

                        assertConverged(gateway.url, reader)

                        // Whether the server ever SEES pressure depends on when the kernel stops
                        // absorbing the flood, so the repair count is not fixed. What is fixed is
                        // §3's promise: every repair snapshot is announced by a resync first. (Before
                        // the closeout fix this ran at 0-1 resyncs for 3-5 snapshots, because the
                        // resync was written into the very backpressure that triggered it.)
                        val resyncs = spy.countOf("resync")
                        val snapshots = spy.countOf("snapshot")
                        println("S6: repaired with $resyncs resync(s), $snapshots snapshot(s)")
                        assertTrue(
                            resyncs >= snapshots - 1,
                            "§3: a fresh snapshot follows a resync — got $resyncs resync(s) for $snapshots snapshot(s)",
                        )
                        assertEquals(40, (serverValue(gateway.url, "room") as JsonObject).size, "the whole flood landed")
                    }
                }
            } finally {
                gateway.stop()
            }
        }
    }

    // ---------------------------------------------------------------- 9

    @Test
    fun `S9 pending overlay - unacked local writes and concurrent foreign deltas coexist`() {
        val gateway = GatewayProcess.start()
        Proxy.start(gateway.port).use { proxy ->
            try {
                rtdbClient(gateway.url, limits = fast).use { writer ->
                    rtdbClient(proxy.url, limits = fast).use { mine ->
                        val values = ValueRecorder()
                        mine.ref("shared").addValueEventListener(values)
                        values.awaitValue(JsonNull)

                        proxy.pauseDownstream() // no acks, no deltas: every local write stays pending
                        val writes = WriteRecorder()
                        for (i in 0 until 5) {
                            mine.ref("shared/a$i").setValue(JsonPrimitive("mine-$i"), writes.callback)
                        }
                        for (i in 0 until 5) writer.ref("shared/b$i").setValue(JsonPrimitive("theirs-$i"))
                        waitUntil("the foreign writes to land") {
                            serverValue(gateway.url, "shared/b4") == JsonPrimitive("theirs-4")
                        }
                        waitUntil("every local write to be pending") { mine.pendingWriteIds.size == 5 }

                        // While pending: the overlay shows every local write, serverState none of them.
                        for (i in 0 until 5) {
                            assertEquals(
                                JsonPrimitive("mine-$i"),
                                mine.value("shared/a$i"),
                                "view = serverState ⊕ overlay (§7)",
                            )
                            assertEquals(
                                JsonNull,
                                mine.mirror.serverValue("shared/a$i"),
                                "serverState is untouched until the echo",
                            )
                        }

                        proxy.cut() // the writes replay on the reconnect (§6 step 3)
                        proxy.resumeDownstream()

                        writes.awaitCount(5, timeoutMs = 30_000)
                        assertTrue(
                            writes.results.all { it is WriteResult.Committed },
                            "every write settled: ${writes.results}",
                        )
                        assertConverged(gateway.url, mine, writer)
                    }
                }
            } finally {
                gateway.stop()
            }
        }
    }

    // ---------------------------------------------------------------- 10

    @Test
    fun `S10 gateway restart WITH persistence - clients back off, resume and replay (§6)`() {
        val directory = Files.createTempDirectory("rtdb-chaos-").toFile()
        val gateway = GatewayProcess.start(persist = File(directory, "oplog.jsonl"))
        try {
            rtdbClient(gateway.url, limits = fast).use { a ->
                rtdbClient(gateway.url, limits = fast).use { b ->
                    val seenByA = ValueRecorder()
                    val seenByB = ValueRecorder()
                    a.ref("room").addValueEventListener(seenByA)
                    b.ref("room").addValueEventListener(seenByB)
                    val epochBefore = a.epoch

                    a.ref("room/before").setValue(JsonPrimitive(1))
                    seenByB.awaitValue(json("""{"before":1}"""))

                    gateway.kill() // SIGKILL: no close frames, the process simply stops
                    downFor(a)

                    val queued = WriteRecorder()
                    a.ref("room/during").setValue(JsonPrimitive(2), queued.callback)
                    waitUntil("the optimistic overlay") { a.value("room") == json("""{"before":1,"during":2}""") }
                    assertEquals(
                        JsonNull,
                        a.mirror.serverValue("room/during"),
                        "the overlay shows it with no server anywhere in sight; serverState does not",
                    )

                    gateway.restart()
                    queued.awaitCount(1, timeoutMs = 30_000)
                    assertTrue(queued.results.single() is WriteResult.Committed, "the pending write replayed")

                    a.ref("room/after").setValue(JsonPrimitive(3))
                    seenByB.awaitValue(json("""{"before":1,"during":2,"after":3}"""), timeoutMs = 30_000)
                    assertEquals(epochBefore, a.epoch, "the data survived, so the generation did not change")
                    assertConverged(gateway.url, a, b)
                }
            }
        } finally {
            gateway.stop()
            directory.deleteRecursively()
        }
    }

    // ---------------------------------------------------------------- 11

    @Test
    fun `S11 gateway restart WITHOUT persistence - the epoch changes and the dead generation is dropped`() {
        val gateway = GatewayProcess.start()
        try {
            rtdbClient(gateway.url, limits = fast).use { client ->
                val values = ValueRecorder()
                client.ref("room").addValueEventListener(values)
                val before = WriteRecorder()
                client.ref("room/before").setValue(JsonPrimitive(1), before.callback)
                // Settled, deliberately: §2 v1.5 says PENDING writes replay against the restored
                // shard and commit as new writes. This scenario is about what the drop throws away,
                // so the write must be acked and out of the queue before the shard dies.
                before.awaitCount(1)
                values.awaitValue(json("""{"before":1}"""))
                waitUntil("the write to leave the queue") { client.pendingWriteIds.isEmpty() }
                val epochBefore = client.epoch

                gateway.restart() // head back to 0: every rev this client holds is from a dead generation

                waitUntil("a new epoch", 30_000) { client.epoch != epochBefore }
                // Without the wholesale drop, §7's per-leaf LWW keeps `before` (rev 1) in preference
                // to the restored shard's rev-0 snapshot, and the client diverges silently.
                waitUntil("the wholesale drop", 30_000) { client.mirror.serverValue("room") == JsonNull }

                val writes = WriteRecorder()
                client.ref("room/after").setValue(JsonPrimitive(2), writes.callback)
                writes.awaitCount(1, timeoutMs = 30_000)
                values.awaitValue(json("""{"after":2}"""), timeoutMs = 30_000)
                assertEquals(
                    json("""{"after":2}"""),
                    client.value("room"),
                    "nothing from the dead generation survived",
                )
                assertConverged(gateway.url, client)
            }
        } finally {
            gateway.stop()
        }
    }
}
