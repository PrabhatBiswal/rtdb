package com.hobostays.rtdb.core

import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/** §Transport tolerance rules and the §1–§10 frame shapes, ported from WP1's `frames.test.ts`. */
class FramesTest {

    @Test
    fun `every client frame encodes as one object with a type field`() {
        assertEquals(
            """{"type":"hello","token":"t","proto":1,"sdk":"kotlin/0.1.0"}""",
            Hello(token = "t", sdk = "kotlin/0.1.0").encode(),
        )
        assertEquals("""{"type":"unlisten","subId":7}""", Unlisten(subId = 7).encode())
        assertEquals("""{"type":"ping","t":42}""", Ping(t = 42).encode())
        assertEquals(
            """{"type":"cas","writeId":"w","path":"p/score","expectedRev":184224,"value":51}""",
            Cas(writeId = "w", path = "p/score", expectedRev = 184224, value = JsonPrimitive(51)).encode(),
        )
    }

    @Test
    fun `an absent lastRev is omitted, not sent as null (§3 no-or-zero means snapshot)`() {
        assertEquals("""{"type":"listen","subId":1,"path":"room"}""", Listen(subId = 1, path = "room").encode())
        assertEquals(
            """{"type":"listen","subId":1,"path":"room","lastRev":184190}""",
            Listen(subId = 1, path = "room", lastRev = 184190).encode(),
        )
    }

    @Test
    fun `a put carries null verbatim — it is the wire form of removeValue (§4)`() {
        assertEquals(
            """{"type":"put","writeId":"w","path":"p","value":null}""",
            Put(writeId = "w", path = "p", value = JsonNull).encode(),
        )
    }

    @Test
    fun `unknown fields are ignored (§Transport)`() {
        val frame = parseServerFrame(
            """{"type":"helloAck","rev":7,"epoch":3,"region":"r","session":"s_1","futureField":{"x":1}}""",
        )
        assertEquals(HelloAck(rev = 7, epoch = 3, region = "r", session = "s_1"), frame)
    }

    @Test
    fun `unknown frame types are ignored, never errors (§Transport)`() {
        assertNull(parseServerFrame("""{"type":"getAck","reqId":3,"value":1,"rev":9}"""), "§11 extension frame")
        assertNull(parseServerFrame("""{"type":"somethingFromV2"}"""))
        assertNull(parseServerFrame("not json at all"))
        assertNull(parseServerFrame("""{"no":"type"}"""))
        assertNull(parseServerFrame("""{"type":"ack","writeId":"w"}"""), "a known type missing a field")
    }

    @Test
    fun `an unknown frame inside a batch does not take its batch-mates with it (§3)`() {
        val batch = parseServerFrame(
            """{"type":"batch","frames":[
                {"type":"delta","rev":1,"path":"p","op":"put","value":1},
                {"type":"fromTheFuture","x":1},
                {"type":"ack","writeId":"w","rev":2}
            ]}""",
        )
        assertIs<Batch>(batch)
        val inner = batch.frames.mapNotNull { parseServerFrame(it) }
        assertEquals(listOf(Delta(rev = 1, path = "p", op = "put", value = JsonPrimitive(1)), Ack("w", 2)), inner)
    }

    @Test
    fun `a delta keeps an unknown op instead of failing to parse (§3 re-listen rule)`() {
        val delta = parseServerFrame("""{"type":"delta","rev":9,"path":"p","op":"incr","value":5}""")
        assertIs<Delta>(delta)
        assertEquals("incr", delta.op, "the sub goes stale and re-listens; the frame itself still parses")
    }

    @Test
    fun `err parses at every scope (§2, §3, §4)`() {
        assertEquals(Err(code = "AUTH", msg = "token expired"), parseServerFrame("""{"type":"err","code":"AUTH","msg":"token expired"}"""))
        assertEquals(Err(code = "RULES", msg = "read denied", subId = 7), parseServerFrame("""{"type":"err","code":"RULES","msg":"read denied","subId":7}"""))
        assertEquals(Err(code = "RATE", msg = "", writeId = "w"), parseServerFrame("""{"type":"err","code":"RATE","writeId":"w"}"""))
    }

    @Test
    fun `server frames round-trip the values the mirror will need (§3, §4)`() {
        val snapshot = parseServerFrame("""{"type":"snapshot","subId":7,"path":"p","value":{"name":"Ravi","score":42},"rev":184223}""")
        assertIs<Snapshot>(snapshot)
        assertEquals(buildJsonObject { put("name", JsonPrimitive("Ravi")); put("score", JsonPrimitive(42)) }, snapshot.value)
        assertEquals(184223, snapshot.rev)

        assertEquals(Resync(subId = 7), parseServerFrame("""{"type":"resync","subId":7}"""))
        assertEquals(Pong(t = 1756280000000), parseServerFrame("""{"type":"pong","t":1756280000000}"""))
        val casFail = parseServerFrame("""{"type":"casFail","writeId":"w","path":"p","value":50,"rev":184224}""")
        assertIs<CasFail>(casFail)
        assertEquals(JsonPrimitive(50), casFail.value)
    }

    @Test
    fun `a merge value is an object of possibly deep keys (§4)`() {
        val merge = Merge(
            writeId = "w",
            path = "p",
            value = buildJsonObject {
                put("score", JsonPrimitive(50))
                put("stats/wins", JsonPrimitive(3))
                put("tag", JsonNull)
            },
        )
        assertTrue(merge.encode().contains(""""stats/wins":3"""))
        assertTrue(merge.encode().contains(""""tag":null"""), "a null child deletes; it must survive encoding")
    }
}
