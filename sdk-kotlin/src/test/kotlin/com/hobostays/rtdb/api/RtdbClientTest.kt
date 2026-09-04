package com.hobostays.rtdb.api

import java.util.concurrent.CopyOnWriteArrayList
import com.hobostays.rtdb.ChildRecorder
import com.hobostays.rtdb.FakeTransport
import com.hobostays.rtdb.ManualDispatcher
import com.hobostays.rtdb.ValueRecorder
import com.hobostays.rtdb.WriteRecorder
import com.hobostays.rtdb.awaitState
import com.hobostays.rtdb.json
import com.hobostays.rtdb.sentOf
import com.hobostays.rtdb.sentTypes
import com.hobostays.rtdb.waitUntil
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.Close
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Limits
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * §3/§4/§7 client behaviour through a fake socket: the frames a server would have to be coaxed into
 * sending (sub-scoped errs, casFail, an §11 op, a changed epoch) driven directly and deterministically.
 * Convergence against a REAL gateway is [com.hobostays.rtdb.ClientIntegrationTest].
 */
class RtdbClientTest {

    private fun helloAck(epoch: Long = 1, rev: Long = 0) =
        """{"type":"helloAck","rev":$rev,"epoch":$epoch,"region":"test","session":"s_fake"}"""

    private fun snapshot(subId: Int, path: String, value: String, rev: Long) =
        """{"type":"snapshot","subId":$subId,"path":"$path","value":$value,"rev":$rev}"""

    private fun delta(rev: Long, path: String, value: String, op: String = "put") =
        """{"type":"delta","rev":$rev,"path":"$path","op":"$op","value":$value}"""

    private fun fixture(): Pair<RtdbClient, FakeTransport> {
        val wire = FakeTransport()
        val client = RtdbClient(
            ConnectionOptions(
                url = "ws://fake",
                token = "t",
                limits = Limits(backoffCapMs = 20),
                pingIntervalMs = 60_000,
            ),
            transports = wire,
        )
        return client to wire
    }

    /** Connect and complete hello, leaving the client CONNECTED with a live fake socket. */
    private fun RtdbClient.bringUp(wire: FakeTransport, epoch: Long = 1) {
        connect()
        waitUntil("connecting") { wire.connects.get() >= 1 }
        wire.open()
        wire.deliver(helloAck(epoch = epoch))
        awaitState(ClientState.CONNECTED)
    }

    @Test
    fun `a listener sends listen, and the snapshot fires onValue from the mirror (§3, §7)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            client.ref("room").addValueEventListener(values)

            waitUntil("the listen frame") { wire.sentOf("listen").isNotEmpty() }
            val listen = wire.sentOf("listen").single()
            assertEquals("room", listen["path"]!!.jsonPrimitive.content)
            assertTrue("lastRev" !in listen, "a fresh sub asks for a snapshot (§3)")

            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            values.awaitValue(json("""{"a":1}"""))
        }
    }

    @Test
    fun `a second listener on a live path fires immediately from the mirror, with no second listen`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val first = ValueRecorder()
            client.ref("room").addValueEventListener(first)
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            first.awaitValue(json("""{"a":1}"""))

            val second = ValueRecorder()
            client.ref("room").addValueEventListener(second)
            second.awaitValue(json("""{"a":1}"""))
            assertEquals(1, wire.sentOf("listen").size, "one path, one subscription")
        }
    }

    @Test
    fun `child events are derived by diffing direct children (§7)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val children = ChildRecorder()
            client.ref("room").addChildEventListener(children)

            wire.deliver(snapshot(1, "room", """{"a":1,"b":2}""", 7))
            waitUntil("initial children") { children.events.size == 2 }
            wire.deliver(delta(8, "room/c", "3"))
            wire.deliver(delta(9, "room/a", "9"))
            wire.deliver(delta(10, "room/b", "null"))
            waitUntil("every child event") { children.events.size == 5 }

            assertEquals(
                listOf("added:a=1", "added:b=2", "added:c=3", "changed:a=9", "removed:b=2"),
                children.events,
            )
        }
    }

    @Test
    fun `an optimistic write is visible before its ack and settles on it (§4, §7)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            client.ref("room").addValueEventListener(values)
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            values.awaitValue(json("""{"a":1}"""))

            val writes = WriteRecorder()
            client.ref("room/b").setValue(JsonPrimitive(2), writes.callback)
            values.awaitValue(json("""{"a":1,"b":2}"""))
            waitUntil("the write is pending") { client.pendingWriteIds.size == 1 }
            assertEquals(json("""{"a":1}"""), client.mirror.serverValue("room"), "serverState is untouched")

            val writeId = wire.sentOf("put").single()["writeId"]!!.jsonPrimitive.content
            wire.deliver("""{"type":"ack","writeId":"$writeId","rev":8}""")
            writes.awaitCount(1)
            assertEquals(WriteResult.Committed(8), writes.results.single())
            waitUntil("the queue clears") { client.pendingWriteIds.isEmpty() }
            // The value stays: the overlay left, and the server echo arrives as its own delta.
            wire.deliver(delta(8, "room/b", "2"))
            values.awaitValue(json("""{"a":1,"b":2}"""))
            assertEquals(json("""{"a":1,"b":2}"""), client.mirror.serverValue("room"))
        }
    }

    @Test
    fun `onValue fires only when the value actually changed (Gate C ruling Q2)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            client.ref("room/player").addValueEventListener(values)
            wire.deliver(snapshot(1, "room/player", """{"score":1}""", 7))
            values.awaitValue(json("""{"score":1}"""))

            // A sibling under the parent: relevant to nothing this sub renders.
            wire.deliver(delta(8, "room/other", "\"x\""))
            // The same delta twice: §3 says apply idempotently, not report twice.
            wire.deliver(delta(9, "room/player/score", "2"))
            wire.deliver(delta(9, "room/player/score", "2"))
            // ...and a stale one §7's LWW drops entirely.
            wire.deliver(delta(5, "room/player/score", "99"))
            values.awaitValue(json("""{"score":2}"""))
            Thread.sleep(80)

            assertEquals(
                listOf(json("""{"score":1}"""), json("""{"score":2}""")),
                values.values.toList(),
                "two real changes, four no-ops",
            )
        }
    }

    @Test
    fun `a child listener sees no event when a change leaves its children identical`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val children = ChildRecorder()
            client.ref("room").addChildEventListener(children)
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            waitUntil("child_added") { children.events.size == 1 }

            wire.deliver(delta(8, "room/a", "1")) // the same value again
            Thread.sleep(80)

            assertEquals(listOf("added:a=1"), children.events, "an unchanged child is not a child_changed")
        }
    }

    @Test
    fun `a casFail settles as Rejected with the state that beat it (§4)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val writes = WriteRecorder()
            client.ref("p/score").compareAndSet(expectedRev = 5, value = JsonPrimitive(51), onComplete = writes.callback)
            waitUntil("the cas frame") { wire.sentOf("cas").isNotEmpty() }
            val writeId = wire.sentOf("cas").single()["writeId"]!!.jsonPrimitive.content

            wire.deliver("""{"type":"casFail","writeId":"$writeId","path":"p/score","value":60,"rev":9}""")
            writes.awaitCount(1)

            assertEquals(WriteResult.Rejected(JsonPrimitive(60), 9), writes.results.single())
            waitUntil("the overlay clears") { client.value("p/score") == JsonNull }
        }
    }

    @Test
    fun `a write err settles as Failed and is never auto-retried (§4)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val writes = WriteRecorder()
            client.ref("p").setValue(JsonPrimitive(1), writes.callback)
            waitUntil("the put frame") { wire.sentOf("put").isNotEmpty() }
            val writeId = wire.sentOf("put").single()["writeId"]!!.jsonPrimitive.content

            wire.deliver("""{"type":"err","writeId":"$writeId","code":"RULES","msg":"write denied"}""")
            writes.awaitCount(1)

            assertEquals(WriteResult.Failed(RtdbError("RULES", "write denied")), writes.results.single())
            assertTrue(client.pendingWriteIds.isEmpty())
            Thread.sleep(50)
            assertEquals(1, wire.sentOf("put").size, "a rejected write leaves the queue for good")
        }
    }

    @Test
    fun `the resume order after a reconnect is hello, listens with lastRev, then pending writes (§6)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            client.ref("room").addValueEventListener(ValueRecorder())
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            waitUntil("the snapshot lands") { client.value("room") == json("""{"a":1}""") }

            wire.drop() // the wire dies with a write in flight
            client.ref("room/b").setValue(JsonPrimitive(2))
            waitUntil("reconnect") { wire.connects.get() == 2 }
            wire.sent.clear()
            wire.open()
            wire.deliver(helloAck())

            waitUntil("the resume frames") { wire.sentTypes().containsAll(listOf("hello", "listen", "put")) }
            val order = wire.sentTypes().filter { it != "ping" }
            assertEquals(listOf("hello", "listen", "put"), order, "§6: hello, then listens, then pending writes")
            assertEquals(
                7L,
                wire.sentOf("listen").last()["lastRev"]!!.jsonPrimitive.content.toLong(),
                "a resume carries the stored lastRev (§3 catch-up)",
            )
        }
    }

    @Test
    fun `an epoch change drops the mirror and re-listens with no lastRev (§2 v1_5)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire, epoch = 1)
            val values = ValueRecorder()
            client.ref("room").addValueEventListener(values)
            wire.deliver(snapshot(1, "room", """{"before":1}""", 7))
            values.awaitValue(json("""{"before":1}"""))

            wire.drop()
            waitUntil("reconnect") { wire.connects.get() == 2 }
            wire.sent.clear()
            wire.open()
            wire.deliver(helloAck(epoch = 2)) // the shard was restored under us

            waitUntil("the re-listen") { wire.sentOf("listen").isNotEmpty() }
            assertTrue(
                "lastRev" !in wire.sentOf("listen").last(),
                "§2 v1.5: every stored lastRev is dropped, so the sub asks for a fresh snapshot",
            )
            assertEquals(JsonNull, client.mirror.serverValue("room"), "the dead generation is gone")
            // ...and the restored shard's low-rev snapshot applies, where §7's LWW would have refused it.
            wire.deliver(snapshot(1, "room", """{"after":2}""", 1))
            values.awaitValue(json("""{"after":2}"""))
        }
    }

    @Test
    fun `a resync does not clear serverState before its snapshot arrives (§3)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            client.ref("room").addValueEventListener(values)
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            values.awaitValue(json("""{"a":1}"""))

            // The server declared this subscription stale; the fresh snapshot follows it.
            wire.deliver("""{"type":"resync","subId":1}""")
            Thread.sleep(50)
            assertEquals(json("""{"a":1}"""), client.value("room"), "no flicker to null in between")

            wire.deliver(snapshot(1, "room", """{"b":2}""", 9))
            values.awaitValue(json("""{"b":2}"""))
            assertEquals(listOf(json("""{"a":1}"""), json("""{"b":2}""")), values.values.toList(), "and no empty value was ever published")
        }
    }

    @Test
    fun `a sub-scoped err cancels that subscription only (§3)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val denied = ValueRecorder()
            val allowed = ValueRecorder()
            client.ref("secret").addValueEventListener(denied)
            client.ref("room").addValueEventListener(allowed)
            waitUntil("both listens") { wire.sentOf("listen").size == 2 }
            val subId = wire.sentOf("listen").first()["subId"]!!.jsonPrimitive.content

            wire.deliver("""{"type":"err","subId":$subId,"code":"RULES","msg":"read denied"}""")
            waitUntil("the cancellation") { denied.cancels.isNotEmpty() }

            assertEquals(RtdbError("RULES", "read denied"), denied.cancels.single())
            assertEquals(listOf("room"), client.subscriptionPaths, "the other subscription is untouched")
            wire.deliver(snapshot(2, "room", """{"a":1}""", 7))
            allowed.awaitValue(json("""{"a":1}"""))
            assertTrue(denied.values.isEmpty())
        }
    }

    @Test
    fun `an unknown delta op re-listens instead of guessing (§3)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            client.ref("p").addValueEventListener(values)
            wire.deliver(snapshot(1, "p", """{"n":1}""", 7))
            values.awaitValue(json("""{"n":1}"""))

            wire.deliver(delta(8, "p/n", "5", op = "incr")) // an §11 op this SDK does not implement
            waitUntil("the re-listen") { wire.sentOf("listen").size == 2 }

            assertTrue(
                "lastRev" !in wire.sentOf("listen").last(),
                "re-listening with the old lastRev would just replay the op we cannot read",
            )
            assertEquals(json("""{"n":1}"""), client.value("p"), "and nothing was guessed at meanwhile")
        }
    }

    @Test
    fun `info connected tracks the connection, served entirely client-side (§7)`() {
        val (client, wire) = fixture()
        client.use {
            val info = ValueRecorder()
            client.ref(INFO_CONNECTED).addValueEventListener(info)
            waitUntil("the initial value") { info.values.isNotEmpty() }
            assertEquals(JsonPrimitive(false), info.values.first())

            client.bringUp(wire)
            info.awaitValue(JsonPrimitive(true))
            wire.drop()
            info.awaitValue(JsonPrimitive(false))
            assertTrue(wire.sentOf("listen").isEmpty(), "a virtual path never reaches the wire")
        }
    }

    @Test
    fun `removing the last listener unlistens the subscription (§3)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            val ref = client.ref("room")
            ref.addValueEventListener(values)
            waitUntil("the listen") { wire.sentOf("listen").size == 1 }
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            values.awaitValue(json("""{"a":1}"""))

            ref.removeEventListener(values)
            waitUntil("the unlisten") { wire.sentOf("unlisten").size == 1 }
            assertEquals(emptyList(), client.subscriptionPaths)

            // In-flight deltas for a dropped sub route to no listener (§3).
            val before = values.values.size
            wire.deliver(delta(9, "room/a", "2"))
            Thread.sleep(50)
            assertEquals(before, values.values.size)
        }
    }

    @Test
    fun `the Flow variant carries the same values and unlistens when collection stops (§7)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            // Collected off the test thread: the waits below are blocking polls, and a collector
            // sharing their thread would never get to run.
            val collected = CopyOnWriteArrayList<DataSnapshot>()
            val collecting = CoroutineScope(Dispatchers.Default).launch {
                client.ref("room").values().take(2).toList(collected)
            }
            waitUntil("the listen frame") { wire.sentOf("listen").isNotEmpty() }
            wire.deliver(snapshot(1, "room", """{"a":1}""", 7))
            waitUntil("the snapshot lands") { client.value("room") == json("""{"a":1}""") }
            // Wait for the COLLECTOR, not just the mirror: values() is conflated on purpose (a
            // value listener's contract is "the current subtree", so a slow collector skips to the
            // latest). Delivering the delta while the first value is still in flight would conflate
            // the two into one emission — take(2) would then wait for a second that never comes.
            waitUntil("the first value to reach the collector") { collected.size == 1 }
            wire.deliver(delta(8, "room/b", "2"))
            runBlocking { withTimeout(5_000) { collecting.join() } }

            assertEquals(listOf(json("""{"a":1}"""), json("""{"a":1,"b":2}""")), collected.map { it.value })
            waitUntil("the unlisten once the flow is done") { wire.sentOf("unlisten").size == 1 }
        }
    }

    @Test
    fun `a bad path or a bad key fails at the call site, not on the wire (§1)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            for (bad in listOf("a//b", "a/.hidden", "a/b#c", "a/b c")) {
                try {
                    client.ref(bad)
                    throw AssertionError("\"$bad\" should not be a legal ref")
                } catch (expected: IllegalArgumentException) {
                    // §1: rejected locally, without costing a round trip
                }
            }
            try {
                client.ref("p").setValue(json("""{"bad.key":1}"""))
                throw AssertionError("a bad key inside a value should not be sent")
            } catch (expected: IllegalArgumentException) {
                // as above, for keys the path validation only sees once flattened
            }
            assertTrue(wire.sentOf("put").isEmpty())
        }
    }
    @Test
    fun `retryNow() through the client short-circuits the wait and re-runs the resume order (§6)`() {
        val wire = FakeTransport()
        // No FixedRandom here — RtdbClient does not expose one, and adding a constructor parameter
        // to a signed-off class for a test is not worth it: the deterministic "the wait really was
        // skipped" assertion lives one layer down in ConnectionTest. What this test owns is the
        // delegation and what follows it — §6's resume order re-running on the connection retryNow
        // brought up. The elapsed bound below still fails ~95% of the time without the seam
        // (§6's first backoff is random(0, 1s), so only a near-zero roll would mimic it).
        val client = RtdbClient(
            ConnectionOptions(url = "ws://fake", token = "t", pingIntervalMs = 60_000),
            transports = wire,
        )
        client.use {
            client.bringUp(wire)
            val values = ValueRecorder()
            client.ref("room").addValueEventListener(values)
            waitUntil("the first listen") { wire.sentOf("listen").isNotEmpty() }
            wire.deliver(snapshot(1, "room", """{"score":1}""", rev = 7))
            values.awaitValue(json("""{"score":1}"""))

            wire.drop()
            client.awaitState(ClientState.WAITING)
            val started = System.currentTimeMillis()
            client.retryNow()

            waitUntil("an immediate reconnect", timeoutMs = 2_000) { wire.connects.get() == 2 }
            assertTrue(System.currentTimeMillis() - started < 100, "the backoff wait was short-circuited")
            wire.open()
            wire.deliver(helloAck())
            client.awaitState(ClientState.CONNECTED)
            // §6 step 2: the re-listen carries the lastRev the sub already has.
            waitUntil("the re-listen") { wire.sentOf("listen").size == 2 }
            assertEquals(7, wire.sentOf("listen").last()["lastRev"]!!.jsonPrimitive.long)
        }
    }

    @Test
    fun `setBackgrounded() through the client reaches the connection (§5)`() {
        val (client, wire) = fixture()
        client.use {
            client.bringUp(wire)
            assertEquals(false, client.backgrounded)
            client.setBackgrounded(true)
            waitUntil("the cadence switch") { client.backgrounded }
            client.setBackgrounded(false)
            waitUntil("the switch back") { !client.backgrounded }
        }
    }
    @Test
    fun `a write during CONNECTING waits for hello, then replays in the resume order (§2, §6)`() {
        val (client, wire) = fixture()
        client.use {
            client.connect()
            waitUntil("the socket to be created") { wire.connects.get() == 1 }
            // CONNECTING: the socket exists, but hello only goes out from onOpen. Before WP3's F5
            // fix this put was written FIRST, and a real gateway answers a pre-hello frame by
            // closing the connection (server.ts: CLOSE.PRE_HELLO, "expected hello").
            val writes = WriteRecorder()
            client.ref("room").setValue(JsonPrimitive(1), writes.callback)

            Thread.sleep(200)
            assertTrue(wire.sent.isEmpty(), "§2: not a byte before hello, however tempting")
            assertEquals(listOf<WriteResult>(), writes.results, "and the write is not settled either")

            wire.open()
            wire.deliver(helloAck())
            client.awaitState(ClientState.CONNECTED)

            // §6: the write was never lost — it is in the pending queue and goes out on resume.
            waitUntil("the write to reach the wire") { wire.sentOf("put").isNotEmpty() }
            assertEquals(
                listOf("hello", "put"),
                wire.sentTypes().filter { it != "ping" },
                "hello first, then the pending write (§6 step 3)",
            )
            assertEquals(1, wire.sentOf("put").size, "and exactly once — no double send")

            wire.deliver("""{"type":"ack","writeId":"${wire.sentOf("put").single()["writeId"]!!.jsonPrimitive.content}","rev":9}""")
            writes.awaitCount(1)
            assertEquals(WriteResult.Committed(9), writes.results.single())
        }
    }

    // ---------------------------------------------------------- P1 (load test 2026-08-29)

    /**
     * §4 has three outcomes and "never settles" is not one of them. The load test found 1,650 writes
     * in that state; these three pin the two guards that make it impossible. Each asserts the write
     * SETTLED, so a missing guard is a failure, not a quieter pass.
     */
    private val abandoned: WriteResult = WriteResult.Failed(CLOSED_ERROR)

    @Test
    fun `P1 t1 a write issued on a closed client fails at once and never joins the queue`() {
        val (client, wire) = fixture()
        client.bringUp(wire)
        client.close()

        val writes = WriteRecorder()
        client.ref("room/a").setValue(JsonPrimitive(1), writes.callback)
        client.ref("room/b").setValue(JsonPrimitive(2), writes.callback)

        writes.awaitCount(2)
        assertEquals(listOf(abandoned, abandoned), writes.results, "every write after close, not just the first")
        assertEquals(JsonNull, client.value("room/a"), "and nothing entered the §7 overlay")
        assertEquals(listOf<String>(), client.pendingWriteIds)
    }

    @Test
    fun `P1 t2 close() fails every pending write - §4 is ack-or-err, never silence`() {
        val (client, wire) = fixture()
        client.bringUp(wire)
        val writes = WriteRecorder()
        client.ref("room/a").setValue(JsonPrimitive(1), writes.callback)
        client.ref("room/b").setValue(JsonPrimitive(2), writes.callback)
        waitUntil("both writes on the wire") { wire.sentOf("put").size == 2 }
        assertEquals(listOf<WriteResult>(), writes.results, "in flight: no ack has come back yet")

        client.close() // ...and now none ever will

        writes.awaitCount(2)
        assertEquals(listOf(abandoned, abandoned), writes.results)
        assertEquals(listOf<String>(), client.pendingWriteIds, "the queue is empty, not merely unsent")
    }

    @Test
    fun `P1 t2b a 4401 abandons the queue too - §6 v1_2 leaves no reconnect to replay it`() {
        val (client, wire) = fixture()
        client.use {
            client.connect()
            waitUntil("the socket to be created") { wire.connects.get() == 1 }
            val writes = WriteRecorder()
            // Issued during CONNECTING: legitimately queued for §6 step 3 — until the 4401 lands and
            // there is no step 3. WP3's F5 guard drops it from the wire; nothing settled it.
            client.ref("room").setValue(JsonPrimitive(1), writes.callback)
            Thread.sleep(100)
            assertEquals(listOf<WriteResult>(), writes.results, "queued, exactly as §6 wants")

            wire.drop(Close.AUTH, "AUTH")
            client.awaitState(ClientState.CLOSED)

            writes.awaitCount(1)
            assertEquals(abandoned, writes.results.single())
        }
    }

    /**
     * F-C1 (mentor finding). The guard used to sit on the CALLER's thread, which made it
     * check-then-act across threads: pass the guard, get preempted, let a teardown sweep the queue,
     * then land. Two windows, both one nanosecond wide, both re-creating the never-settle the whole
     * package exists to remove. The fix has two halves and these two tests take one each — neither
     * needs to win a race, because each reproduces the STATE the race produces.
     */
    private fun manual(): Triple<RtdbClient, FakeTransport, ManualDispatcher> {
        val wire = FakeTransport()
        val dispatcher = ManualDispatcher()
        val client = RtdbClient(
            ConnectionOptions(
                url = "ws://fake",
                token = "t",
                limits = Limits(backoffCapMs = 20),
                pingIntervalMs = 60_000,
            ),
            dispatcher = dispatcher,
            transports = wire,
        )
        client.connect()
        dispatcher.drain()
        wire.open()
        dispatcher.drain()
        wire.deliver(helloAck())
        dispatcher.drain()
        assertEquals(ClientState.CONNECTED, client.state.value, "the fixture should be up")
        return Triple(client, wire, dispatcher)
    }

    @Test
    fun `P1 t3 a write landing AFTER the terminal sweep settles instead of stranding in the queue`() {
        // Trace B: the 4401 half. The FSM is CLOSED and its sweep has already run, so a block
        // arriving now would be added to a queue nothing will ever look at again. The scope is
        // alive here — this is the half that `invokeOnCompletion` cannot cover.
        val (client, wire, dispatcher) = manual()
        wire.drop(Close.AUTH, "AUTH")
        dispatcher.drain()
        assertEquals(ClientState.CLOSED, client.state.value)

        val writes = WriteRecorder()
        client.ref("room").setValue(JsonPrimitive(1), writes.callback)
        dispatcher.drain()

        assertEquals(listOf(abandoned), writes.results.toList())
        assertEquals(listOf<String>(), client.pendingWriteIds, "and it never joined the queue at all")
    }

    @Test
    fun `P1 t3b a write whose block close() cancels before it starts settles anyway`() {
        // Trace A: the close() half. The scope is cancelled, so `scope.launch` returns a job whose
        // body will never be entered — the write vanishes with not even a queue entry to find it by.
        // The in-block check cannot help; only launch-OR-settle can.
        val (client, _, dispatcher) = manual()
        client.close()
        dispatcher.drain() // failPending, Connection's own close, and scope.cancel()

        val writes = WriteRecorder()
        client.ref("room").setValue(JsonPrimitive(1), writes.callback)
        dispatcher.drain()

        assertEquals(listOf(abandoned), writes.results.toList())
    }
}
