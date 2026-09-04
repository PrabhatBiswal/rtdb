package com.hobostays.rtdb

import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Limits
import java.io.BufferedReader
import java.io.File
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

/**
 * WORKLOAD §6 Gate C: a Kotlin client and WP1's TypeScript harness client, both real protocol
 * citizens on the same gateway, must end up holding the same tree. Two independent implementations
 * of §3/§4/§7 agreeing is the check neither one can do alone.
 */
class CrossSdkTest {

    /** WP1's `harness/client.ts`, driven a line at a time (see `harness/peer.ts`). */
    private class TsPeer private constructor(private val process: Process) : AutoCloseable {
        private val out: BufferedReader = process.inputStream.bufferedReader()
        private val input = process.outputStream.writer()

        companion object {
            private val REPO = File(System.getProperty("user.dir")).parentFile

            fun start(url: String, token: String, path: String): TsPeer {
                val process = ProcessBuilder("node", "--import", "tsx", "harness/peer.ts", url, token, path)
                    .directory(REPO)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start()
                val peer = TsPeer(process)
                check(peer.out.readLine() == "ready") { "the TypeScript peer never came up" }
                return peer
            }
        }

        fun command(line: String): String {
            input.write(line + "\n")
            input.flush()
            return out.readLine() ?: error("the TypeScript peer died on: $line")
        }

        /** The peer's own mirrored view of its listen path. */
        fun dump(): JsonElement =
            Json.parseToJsonElement(command("dump").removePrefix("value "))

        override fun close() {
            runCatching { command("exit") }
            process.destroyForcibly().waitFor()
        }
    }

    @Test
    fun `a Kotlin client and the TypeScript harness client converge on the same tree`() {
        val gateway = GatewayProcess.start()
        val token = devToken()
        try {
            val kotlin = RtdbClient(
                ConnectionOptions(
                    url = gateway.url,
                    token = token,
                    sdk = "kotlin/0.1.0",
                    limits = Limits(backoffCapMs = 40),
                    pingIntervalMs = 60_000,
                ),
            )
            kotlin.use {
                kotlin.connect()
                runBlocking { withTimeout(15_000) { kotlin.ready() } }
                val values = ValueRecorder()
                kotlin.ref("room").addValueEventListener(values)
                values.awaitValue(kotlinx.serialization.json.JsonNull)

                TsPeer.start(gateway.url, token, "room").use { peer ->
                    // Each side writes; each side must see BOTH writes, from its own mirror.
                    kotlin.ref("room/kotlin").setValue(json("""{"sdk":"kotlin","score":42}"""))
                    assertEquals("ok", peer.command("""put ts {"sdk":"ts","score":7}"""))

                    val both = json("""{"kotlin":{"sdk":"kotlin","score":42},"ts":{"sdk":"ts","score":7}}""")
                    values.awaitValue(both, timeoutMs = 15_000)
                    assertEquals("ok", peer.command("await . ${Json.encodeToString(JsonElement.serializer(), both)}"))
                    assertEquals(both, peer.dump(), "the TypeScript mirror agrees leaf for leaf")

                    // A delete crosses the SDK boundary the same way (§4: put with a null value).
                    assertEquals("ok", peer.command("put kotlin null"))
                    val afterDelete = json("""{"ts":{"sdk":"ts","score":7}}""")
                    values.awaitValue(afterDelete, timeoutMs = 15_000)

                    // ...and a deep merge from Kotlin lands identically on the TypeScript side.
                    kotlin.ref("room/ts").updateChildren(mapOf("stats/wins" to JsonPrimitive(3)))
                    val merged = json("""{"ts":{"sdk":"ts","score":7,"stats":{"wins":3}}}""")
                    values.awaitValue(merged, timeoutMs = 15_000)
                    assertEquals("ok", peer.command("await . ${Json.encodeToString(JsonElement.serializer(), merged)}"))
                    assertEquals(merged, peer.dump())
                    // Convergence is eventual by definition: the view is optimistic the moment the
                    // write is issued, serverState only once the echo has crossed the gateway.
                    waitUntil("the Kotlin serverState to catch up", timeoutMs = 15_000) {
                        kotlin.mirror.serverValue("room") == merged
                    }
                    assertEquals(merged, kotlin.mirror.serverValue("room"), "both mirrors, one tree")
                    waitUntil("every write to settle") { kotlin.pendingWriteIds.isEmpty() }
                }
            }
        } finally {
            gateway.stop()
        }
    }
}
