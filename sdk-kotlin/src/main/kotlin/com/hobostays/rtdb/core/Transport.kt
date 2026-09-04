package com.hobostays.rtdb.core

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener

/** One open socket. Everything the FSM needs from the wire, and nothing more (WORKLOAD §4). */
interface Transport {
    fun send(text: String)
    fun close(code: Int, reason: String)
}

/**
 * Callbacks from the socket. They arrive on whatever thread the transport uses — the FSM hops each
 * one onto its own dispatcher.
 *
 * There is deliberately no `onFailure`: a connect that never succeeds and a connection that dies
 * are the same event to §6, and WP1 learned the hard way that treating them differently strands the
 * FSM (a failed CONNECT fires `error` and never fires `close`). Both arrive here as [onClosed].
 */
interface TransportListener {
    fun onOpen()
    fun onText(text: String)
    fun onClosed(code: Int, reason: String)
}

interface TransportFactory {
    fun connect(url: String, listener: TransportListener): Transport

    /** Release whatever the factory owns. Only the owner of the factory calls this. */
    fun shutdown() {}
}

/** The real wire: OkHttp (WORKLOAD §3). */
class OkHttpTransportFactory(
    private val client: OkHttpClient = OkHttpClient(),
) : TransportFactory {

    override fun connect(url: String, listener: TransportListener): Transport {
        val socket = client.newWebSocket(
            Request.Builder().url(url).build(),
            object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) = listener.onOpen()

                override fun onMessage(webSocket: WebSocket, text: String) = listener.onText(text)

                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    // Complete the closing handshake, then report the code the SERVER chose — 4401
                    // and 4403 (§2, §10) are decisions the FSM has to see.
                    webSocket.close(Close.NORMAL, null)
                    listener.onClosed(code, reason)
                }

                override fun onClosed(webSocket: WebSocket, code: Int, reason: String) =
                    listener.onClosed(code, reason)

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) =
                    listener.onClosed(Close.ABNORMAL, t.message ?: "connection failed")
            },
        )
        return object : Transport {
            override fun send(text: String) {
                socket.send(text)
            }

            override fun close(code: Int, reason: String) {
                // close() returns false when there is no handshake left to run — a socket that
                // never opened, or one already going away. Then cancel is the only way out, and
                // leaving it uncancelled leaks the connection.
                if (!socket.close(code, reason)) socket.cancel()
            }
        }
    }

    override fun shutdown() {
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }
}
