package com.hobostays.rtdb

import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

/**
 * A TCP relay in front of the gateway, so tests can break the network without any test-only hook in
 * the server — the Kotlin port of WP1's `harness/proxy.ts` (WORKLOAD §5).
 *
 * Three faults, which between them drive the chaos suite: [cut] (the wire just stops),
 * [pauseDownstream] (stop reading server -> client, so real kernel backpressure builds), and
 * [blackhole] (cut, and refuse reconnects — the client backs off against a server that is simply
 * not there).
 */
class Proxy private constructor(
    private val server: ServerSocket,
    private val targetPort: Int,
) : AutoCloseable {

    private val pairs = CopyOnWriteArrayList<Pair<Socket, Socket>>()
    private val gate = Object()

    @Volatile
    private var paused = false

    @Volatile
    private var blocked = false

    val port: Int get() = server.localPort
    val url: String get() = "ws://127.0.0.1:$port"
    val connections: Int get() = pairs.size

    companion object {
        fun start(targetPort: Int): Proxy {
            val proxy = Proxy(ServerSocket(0, 50, InetAddress.getByName("127.0.0.1")), targetPort)
            thread(isDaemon = true, name = "rtdb-proxy") { proxy.acceptLoop() }
            return proxy
        }
    }

    /** Kill every live connection without a close frame — the wire just stops. */
    fun cut() {
        for (pair in pairs) closePair(pair)
    }

    /** Cut, and refuse reconnects: the server is unreachable, not merely slow. */
    fun blackhole() {
        blocked = true
        cut()
    }

    fun restore() {
        blocked = false
    }

    /** Stop draining server -> client. Real backpressure builds behind this. */
    fun pauseDownstream() {
        paused = true
    }

    fun resumeDownstream() = synchronized(gate) {
        paused = false
        gate.notifyAll()
    }

    override fun close() {
        cut()
        server.close()
    }

    private fun acceptLoop() {
        while (!server.isClosed) {
            val client = try {
                server.accept()
            } catch (e: IOException) {
                return // the proxy was closed
            }
            if (blocked) {
                client.close()
                continue
            }
            val upstream = Socket("127.0.0.1", targetPort)
            val pair = client to upstream
            pairs += pair
            thread(isDaemon = true) { pump(pair, client, upstream, downstream = false) }
            thread(isDaemon = true) { pump(pair, upstream, client, downstream = true) }
        }
    }

    private fun pump(pair: Pair<Socket, Socket>, from: Socket, to: Socket, downstream: Boolean) {
        val buffer = ByteArray(16 * 1024)
        try {
            while (true) {
                // Before the read, so bytes pile up in the kernel rather than in this process —
                // that is a slow consumer, not a simulation of one.
                if (downstream) synchronized(gate) { while (paused) gate.wait() }
                val read = from.getInputStream().read(buffer)
                if (read < 0) break
                // ...and again after it: this thread is normally parked INSIDE read(), so a pause
                // that arrives while it is blocked would otherwise let one more chunk through.
                if (downstream) synchronized(gate) { while (paused) gate.wait() }
                to.getOutputStream().apply {
                    write(buffer, 0, read)
                    flush()
                }
            }
        } catch (e: IOException) {
            // The wire died. That is this class's entire purpose.
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        } finally {
            closePair(pair)
        }
    }

    private fun closePair(pair: Pair<Socket, Socket>) {
        pairs.remove(pair)
        for (socket in listOf(pair.first, pair.second)) {
            try {
                socket.close()
            } catch (e: IOException) {
                // already gone
            }
        }
    }
}
