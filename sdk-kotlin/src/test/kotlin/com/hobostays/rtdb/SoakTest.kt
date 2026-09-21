package com.hobostays.rtdb

import com.hobostays.rtdb.api.ChildEvent
import com.hobostays.rtdb.api.DataSnapshot
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.RtdbError
import com.hobostays.rtdb.api.ValueEventListener
import com.hobostays.rtdb.api.childEvents
import com.hobostays.rtdb.core.Limits
import com.hobostays.rtdb.core.OkHttpTransportFactory
import java.util.BitSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.math.max
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * WORKLOAD §5.36 — the SOAK. Everything else in this module runs for seconds; this runs for twenty
 * minutes, with the wire breaking under it the whole time, because the failures it is looking for
 * are the ones that need TIME: a mirror that grows, a tombstone that is never collected, a child
 * stream that delivers something twice after a catch-up, a client that reconnects but never
 * converges again.
 *
 * NOT in the normal battery: `@Tag("soak")`, excluded by `tasks.test` and run by `./gradlew soak`.
 * Every number is a system property, so the same file is the 2-minute smoke and the 20-minute run:
 *
 *   ./gradlew soak                                   # the ordered shape: 50 clients, 20 minutes
 *   ./gradlew soak -Dsoak.clients=10 -Dsoak.minutes=2 # a smoke, ~2 minutes
 *
 * WHAT "50 CLIENTS" MEANS HERE, and it is not what production will see: 50 `RtdbClient` INSTANCES
 * in ONE JVM, sharing ONE [OkHttpTransportFactory]. Constructed the default way each client builds
 * its own `OkHttpClient` (Transport.kt:38) on top of its own single-thread executor
 * (RtdbClient.kt:63) — 50 of those is ~150-200 threads and 50 connection pools, and this laptop
 * has 8 GB with the swap already most of the way gone. Sharing the transport changes nothing that
 * is under test: 50 independent FSMs, 50 independent mirrors, 50 sockets. It does mean this test
 * says nothing about 50 SEPARATE PROCESSES, which is what 50 phones are.
 */
class SoakTest {

    private fun prop(name: String, default: Int): Int =
        System.getProperty(name)?.toIntOrNull() ?: default

    private val clientCount = prop("soak.clients", 50)
    private val minutes = prop("soak.minutes", 20)
    private val writerCount = prop("soak.writers", 5)
    /** Writes per second PER WRITER. */
    private val writeRate = prop("soak.rate", 2)
    /** A small key set, so children churn (added -> changed -> removed -> added) instead of only growing. */
    private val keyCount = prop("soak.keys", 20)
    private val killEverySec = prop("soak.killEverySec", 30)
    private val killCount = prop("soak.killCount", 3)
    /** How many clients record cross-client latency. All of them would be 600k samples for one p99. */
    private val observerCount = prop("soak.observers", 5)

    private val room = "soak/room"

    /**
     * One client's ledger. Everything here is bounded by the KEY SET or by the writer count, never
     * by elapsed time — a soak whose own bookkeeping grows with the run cannot measure growth.
     */
    private class Ledger(writers: Int) {
        /** Seen writes per writer, by sequence number. A BitSet of 2,400 bits, not a set of strings. */
        val seen = Array(writers) { BitSet() }
        val everAdded = ConcurrentHashMap.newKeySet<String>()
        val added = AtomicInteger()
        val changed = AtomicInteger()
        val removed = AtomicInteger()
        /** THE assertion: the same (writer, seq) delivered twice to one client. */
        val duplicates = AtomicInteger()
        /** THE other one: a Removed for a key this client was never told was Added. */
        val removedWithoutAdded = AtomicInteger()
        val cancelled = AtomicInteger()
        /** Cross-client latency samples, in ms. Only the observers fill this. */
        val latencies = ArrayList<Long>()
        /** The first few duplicates, spelled out — a count alone cannot tell a finding from a harness bug. */
        val duplicateDetail = java.util.Collections.synchronizedList(ArrayList<String>())
    }

    /**
     * A value listener that keeps a COUNT and the last value, not a history.
     * `ValueRecorder` appends every value to a `CopyOnWriteArrayList` — at 10 writes/s for 20
     * minutes that is 12,000 array copies per client, which on 50 clients is the test measuring
     * itself.
     */
    private class CountingValues : ValueEventListener {
        val count = AtomicInteger()
        val cancels = AtomicInteger()

        @Volatile
        var last: JsonElement? = null

        override fun onDataChange(snapshot: DataSnapshot) {
            last = snapshot.value
            count.incrementAndGet()
        }

        override fun onCancelled(error: RtdbError) {
            cancels.incrementAndGet()
        }
    }

    @Test
    @Tag("soak")
    fun `50 clients, 20 minutes, sockets dying every 30 seconds - nothing diverges and nothing grows`() {
        val started = System.currentTimeMillis()
        val gateway = GatewayProcess.start()
        // One OkHttpClient for every SDK instance — see the class KDoc for why, and for what it
        // costs in fidelity.
        val shared = OkHttpTransportFactory(OkHttpClient())
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val log = StringBuilder()
        fun say(line: String) {
            val at = (System.currentTimeMillis() - started) / 1000
            val text = "[soak ${at}s] $line"
            println(text)
            log.appendLine(text)
        }

        say(
            "clients=$clientCount writers=$writerCount rate=${writeRate}/s keys=$keyCount " +
                "minutes=$minutes kill=$killCount/${killEverySec}s loadavg=${loadAverage()}",
        )

        Proxy.start(gateway.port).use { proxy ->
            val clients = ArrayList<RtdbClient>(clientCount)
            val ledgers = ArrayList<Ledger>(clientCount)
            val values = ArrayList<CountingValues>(clientCount)
            // Everything reconnects through the proxy, which is the only thing that can break the
            // wire from OUTSIDE the SDK. `backoffCapMs` is deliberately NOT the 40 ms the other
            // suites use: 200 ms is closer to a real client, and at 40 ms a cut client spins.
            val limits = Limits(backoffCapMs = 200)
            try {
                for (i in 0 until clientCount) {
                    val client = rtdbClient(proxy.url, token = devToken("u_soak_$i"), limits = limits, transports = shared)
                    val ledger = Ledger(writerCount)
                    val value = CountingValues()
                    clients += client
                    ledgers += ledger
                    values += value
                    client.ref(room).addValueEventListener(value)
                    // The §5.32 Flow, collected for the whole run — the surface a real app uses for
                    // "show me this list", and the one whose buffer is UNLIMITED for this reason.
                    val isObserver = i in writerCount until (writerCount + observerCount)
                    scope.launch {
                        client.ref(room).childEvents().collect { event -> record(ledger, event, isObserver) }
                    }
                }
                say("${clients.size} clients connected and listening on $room")

                val writersPaused = AtomicBoolean(false)
                val stop = AtomicBoolean(false)
                val writesIssued = AtomicInteger()
                val reconnects = AtomicInteger()
                val cutTotal = AtomicInteger()
                val sequence = Array(writerCount) { AtomicLong() }

                // Writers: the first `writerCount` clients, each at `writeRate` per second, mixing
                // put / merge / remove over a fixed key set so children are added, changed and
                // removed rather than only accumulating.
                val writerThreads = (0 until writerCount).map { w ->
                    thread(isDaemon = true, name = "soak-writer-$w") {
                        val periodMs = 1000L / writeRate
                        while (!stop.get()) {
                            if (!writersPaused.get()) {
                                val n = sequence[w].getAndIncrement()
                                val op = (n % 5L).toInt()
                                // The remove targets the PREVIOUS key, which this writer has just
                                // written. Removing `(w*7+n)` instead meant the remove always
                                // landed on a key nobody ever set — the first smoke read
                                // `removed=0` with one writer, i.e. no tombstones and no Removed
                                // events at all, which is exactly the churn this soak is for.
                                val index = if (op == 4) (w * 7 + n - 1) else (w * 7 + n)
                                val key = "k${Math.floorMod(index, keyCount.toLong())}"
                                val ref = clients[w].ref("$room/$key")
                                when (op) {
                                    // A remove every fifth write, so tombstones are exercised and
                                    // the key set churns instead of only being overwritten.
                                    4 -> ref.removeValue(null)
                                    2 -> ref.updateChildren(
                                        buildJsonObject {
                                            put("w", JsonPrimitive(w))
                                            put("n", JsonPrimitive(n))
                                            put("t", JsonPrimitive(System.currentTimeMillis()))
                                        },
                                        null,
                                    )
                                    else -> ref.setValue(
                                        buildJsonObject {
                                            put("w", JsonPrimitive(w))
                                            put("n", JsonPrimitive(n))
                                            put("t", JsonPrimitive(System.currentTimeMillis()))
                                        },
                                        null,
                                    )
                                }
                                writesIssued.incrementAndGet()
                            }
                            Thread.sleep(periodMs)
                        }
                    }
                }

                // The fault: a few sockets die from outside every `killEverySec`, so catch-up runs
                // continuously rather than once. Paused during a checkpoint, or the settle window
                // would never settle.
                val killer = thread(isDaemon = true, name = "soak-killer") {
                    while (!stop.get()) {
                        Thread.sleep(killEverySec * 1000L)
                        if (stop.get()) break
                        if (writersPaused.get()) continue
                        val cut = proxy.cutSome(killCount)
                        cutTotal.addAndGet(cut)
                        reconnects.addAndGet(cut)
                    }
                }

                var heapAtMinute2 = 0L
                var cellsAtMinute2 = 0
                val deadline = started + minutes * 60_000L
                var checkpoint = 0
                while (System.currentTimeMillis() < deadline) {
                    Thread.sleep(minOf(120_000L, max(1_000L, deadline - System.currentTimeMillis())))
                    checkpoint++
                    val label = "checkpoint $checkpoint"
                    // Convergence is only a question that HAS an answer when the tree stops moving,
                    // so the writers and the killer stand down for a settle window. Everything else
                    // — the listeners, the collectors, the reconnect backoff — keeps running.
                    writersPaused.set(true)
                    Thread.sleep(4_000)
                    val expected = converge(gateway, clients, label, ::say)
                    val cells = clients.sumOf { it.mirror.cellCount }
                    val tombs = clients.sumOf { it.mirror.tombstoneCount }
                    val heap = heapAfterGc()
                    say(
                        "$label: converged on ${expected.size()} bytes of tree; cells=$cells " +
                            "tombstones=$tombs heap=${heap / 1024 / 1024}MB gatewayRss=${rssMb(gateway.pid)}MB " +
                            "writes=${writesIssued.get()} cuts=${cutTotal.get()} loadavg=${loadAverage()}",
                    )
                    if (checkpoint == 1) {
                        heapAtMinute2 = heap
                        cellsAtMinute2 = cells
                    }
                    writersPaused.set(false)
                }

                stop.set(true)
                writersPaused.set(true)
                writerThreads.forEach { it.join(5_000) }
                killer.join(5_000)
                Thread.sleep(3_000)

                val expected = converge(gateway, clients, "final", ::say)
                val cells = clients.sumOf { it.mirror.cellCount }
                val tombs = clients.sumOf { it.mirror.tombstoneCount }
                val heapEnd = heapAfterGc()

                // ---------------------------------------------------------------- the assertions

                // (a) is inside `converge`, which throws on the first client that will not settle.

                // (b) the child stream: nothing delivered twice, nothing removed that was never added.
                // A WRITER's own client is a different case from an observer's, and the smoke run
                // is what separated them. With 5 writers on a shared key set, duplicates appeared
                // ONLY on clients 0..4 and always paired with a competing writer's payload for the
                // same key; with `-Dsoak.writers=1` (no two writers on one key) the count was 0.
                //
                // That is §7 working, not redelivery: `view = serverState + overlay`, so a writer
                // sees its OWN value the instant it writes, then the other writer's value the
                // instant its ack settles and the overlay comes off, then its own again when its
                // echo lands. Three events, two of which repeat a (writer, seq) it was already
                // shown. An app rendering `childEvents()` on a key two clients fight over WILL
                // flicker — that is worth knowing, and it is not divergence: every one of these
                // clients converges at the checkpoint.
                //
                // So the assertion is on the clients that only READ, which is the population the
                // property is actually about.
                val duplicates = ledgers.drop(writerCount).sumOf { it.duplicates.get() }
                val writerFlaps = ledgers.take(writerCount).sumOf { it.duplicates.get() }
                val orphanRemoves = ledgers.sumOf { it.removedWithoutAdded.get() }
                val cancels = ledgers.sumOf { it.cancelled.get() } + values.sumOf { it.cancels.get() }
                ledgers.withIndex().filter { it.value.duplicateDetail.isNotEmpty() }.take(4).forEach { (i, l) ->
                    val kind = if (i < writerCount) "overlay flap (writer client)" else "DUPLICATE"
                    say("$kind on client $i: ${l.duplicateDetail.joinToString(" | ")}")
                }
                assertEquals(
                    0,
                    duplicates,
                    "a (writer, seq) state was delivered twice to a READING client — redelivery, " +
                        "not the overlay (writer clients flapped $writerFlaps times, which is §7)",
                )
                assertEquals(0, orphanRemoves, "a child was removed on a client that was never told it was added")
                assertEquals(0, cancels, "a subscription was cancelled during the soak")

                // (c) the mirror is bounded BY THE DATA, not by the run. The key set is fixed, so a
                // mirror holding more cells than the tree can have is holding history — which is
                // the leak this test exists to catch. Tombstones are part of the bound, not an
                // exception to it: §7 keeps one per removed key, not one per removal.
                val perClientCells = clients.map { it.mirror.cellCount }
                val bound = keyCount * 4 + 8
                assertTrue(
                    perClientCells.all { it <= bound },
                    "a mirror holds more than $bound cells for a $keyCount-key tree: ${perClientCells.max()} " +
                        "(cells grow with TIME, which is the leak; per-client: $perClientCells)",
                )
                if (cellsAtMinute2 > 0) {
                    say("cells at first checkpoint $cellsAtMinute2 -> $cells at the end (bound $bound per client)")
                }

                // (d) reported, not asserted — see the §5.36 cross-question: a heap number on a
                // laptop that is swapping measures the GC's response to memory pressure.
                // The ratio, not just the count: "28 flaps" means nothing without a denominator,
                // and the one an app author can act on is per write ISSUED BY A WRITER CLIENT —
                // how often writing to a contended key shows you a value that is not yours before
                // it settles. The denominator is every write, not only the contended ones, because
                // a writer cannot know in advance which of its keys another client is holding.
                val flapRatio = if (writesIssued.get() > 0) {
                    String.format("%.1f%%", 100.0 * writerFlaps / writesIssued.get())
                } else {
                    "n/a"
                }
                say(
                    "REPORT writes=${writesIssued.get()} cuts=${cutTotal.get()} reconnects=${reconnects.get()} " +
                        "added=${ledgers.sumOf { it.added.get() }} changed=${ledgers.sumOf { it.changed.get() }} " +
                        "removed=${ledgers.sumOf { it.removed.get() }} duplicates(readers)=$duplicates " +
                        "overlayFlaps(writers)=$writerFlaps ($flapRatio of writes) orphanRemoves=$orphanRemoves",
                )
                say(
                    "REPORT heap minute-2=${heapAtMinute2 / 1024 / 1024}MB end=${heapEnd / 1024 / 1024}MB " +
                        "cells=$cells tombstones=$tombs gatewayRss=${rssMb(gateway.pid)}MB loadavg=${loadAverage()}",
                )
                val samples = ledgers.flatMap { it.latencies }.sorted()
                if (samples.isNotEmpty()) {
                    val p = { q: Double -> samples[minOf(samples.size - 1, (samples.size * q).toInt())] }
                    say(
                        "REPORT cross-client latency (write on a writer -> child event on an observer), " +
                            "n=${samples.size} p50=${p(0.50)}ms p99=${p(0.99)}ms max=${samples.last()}ms " +
                            "— OBSERVED UNDER CONTENTION on an 8 GB laptop, loadavg=${loadAverage()}, not a spec number",
                    )
                }
                say("final tree: $expected")
            } finally {
                scope.cancel()
                clients.forEach { runCatching { it.close() } }
                shared.shutdown()
                gateway.stop()
                println(log)
            }
        }
    }

    /** One child event into one client's ledger. Called on the collector's coroutine. */
    private fun record(ledger: Ledger, event: ChildEvent, isObserver: Boolean) {
        val key = event.snapshot.key ?: return
        val body = event.snapshot.value as? JsonObject
        val writer = body?.get("w")?.jsonPrimitive?.content?.toIntOrNull()
        val seq = body?.get("n")?.jsonPrimitive?.content?.toIntOrNull()
        when (event) {
            is ChildEvent.Added -> {
                ledger.added.incrementAndGet()
                ledger.everAdded += key
            }
            is ChildEvent.Changed -> ledger.changed.incrementAndGet()
            is ChildEvent.Removed -> {
                ledger.removed.incrementAndGet()
                // A Removed for a key this client never saw Added is divergence, not churn: the
                // client is being told to delete something it was never shown.
                if (!ledger.everAdded.contains(key)) ledger.removedWithoutAdded.incrementAndGet()
            }
        }
        // (writer, seq) identifies ONE write for the life of the run — sequence numbers only ever
        // go up — so the same pair arriving twice at one client is a redelivery, not churn. The
        // order said "(key, rev)"; a rev is not on the wire at this surface (DataSnapshot carries
        // path and value), and the write's own identity is the stronger key: it survives a key
        // being written, removed and written again.
        //
        // ADDED AND CHANGED ONLY, and the first smoke run is why: `fireChange` reports a Removed
        // with the value the child had BEFORE it went (RtdbClient.kt:436), so every remove
        // re-delivers the payload of the write it is removing. Counting those read 1,252
        // "duplicates" in 60 seconds — one per remove per client, an artifact of this ledger and
        // not a redelivery. A duplicate is a STATE the client was shown twice.
        if (event !is ChildEvent.Removed && writer != null && seq != null && writer < ledger.seen.size) {
            synchronized(ledger.seen) {
                if (ledger.seen[writer].get(seq)) {
                    ledger.duplicates.incrementAndGet()
                    if (ledger.duplicateDetail.size < 12) {
                        ledger.duplicateDetail += "${event::class.simpleName} $key w=$writer n=$seq value=${event.snapshot.value}"
                    }
                } else {
                    ledger.seen[writer].set(seq)
                }
            }
        }
        if (isObserver && body != null) {
            val sent = body["t"]?.jsonPrimitive?.content?.toLongOrNull()
            if (sent != null) synchronized(ledger.latencies) { ledger.latencies += System.currentTimeMillis() - sent }
        }
    }

    /**
     * §5.36 (a): every client's mirror equals the server's, read ONCE per round.
     *
     * `assertConverged` opens a fresh probe client per client per path — at 50 clients over 11
     * rounds that is 550 extra connects and hellos INTO the thing being measured. The server's
     * value does not depend on which client is asking, so it is read once here.
     */
    private fun converge(
        gateway: GatewayProcess,
        clients: List<RtdbClient>,
        label: String,
        say: (String) -> Unit,
    ): JsonElement {
        // Straight to the gateway, not through the proxy: the probe must not be a casualty of the
        // next cut, and it is not part of the population under test.
        val expected = serverValue(gateway.url, room)
        var slowest = 0L
        for ((index, client) in clients.withIndex()) {
            val began = System.currentTimeMillis()
            waitUntil("$label: client $index to settle every write", 30_000) { client.pendingWriteIds.isEmpty() }
            waitUntil("$label: client $index to converge at \"$room\"", 30_000) {
                client.mirror.serverValue(room) == expected
            }
            assertEquals(expected, client.mirror.serverValue(room), "$label: client $index diverged")
            slowest = max(slowest, System.currentTimeMillis() - began)
        }
        say("$label: all ${clients.size} mirrors == the server, slowest client took ${slowest}ms")
        return expected
    }

    /** Best-effort heap-after-GC. Reported, never asserted — see the class KDoc. */
    private fun heapAfterGc(): Long {
        System.gc()
        Thread.sleep(200)
        System.gc()
        val runtime = Runtime.getRuntime()
        return runtime.totalMemory() - runtime.freeMemory()
    }

    /** The gateway is another OS process; its RSS is the OS's answer, not the JVM's. */
    private fun rssMb(pid: Long): Long = runCatching {
        ProcessBuilder("ps", "-o", "rss=", "-p", pid.toString())
            .start().inputStream.bufferedReader().readText().trim().toLong() / 1024
    }.getOrDefault(-1)

    /** The number that says whether a latency reading tonight means anything. */
    private fun loadAverage(): String = runCatching {
        ProcessBuilder("sysctl", "-n", "vm.loadavg").start()
            .inputStream.bufferedReader().readText().trim()
    }.getOrDefault("?")

    private fun JsonElement.size(): Int = toString().length
}
