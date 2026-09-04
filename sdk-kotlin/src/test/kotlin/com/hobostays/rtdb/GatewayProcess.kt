package com.hobostays.rtdb

import java.io.BufferedReader
import java.io.File
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import okio.ByteString.Companion.toByteString

/**
 * The WP1 Node gateway as a real, killable OS process — the port of `harness/scenario.ts`'s
 * GatewayProcess (WORKLOAD §2: integration tests run against the real gateway, no mock servers).
 * `src/gateway/main.ts` prints `rtdb listening <port>` on stdout once it is bound.
 */
class GatewayProcess private constructor(
    private val limitsJson: String,
    private val persist: File?,
) {
    private var process: Process? = null

    /** Fixed after the first spawn, so a restart reuses the port clients already hold. */
    var port: Int = 0
        private set

    val url: String get() = "ws://127.0.0.1:$port"

    companion object {
        /** rtdb/ — the repo root, two levels up from sdk-kotlin's working directory. */
        private val REPO = File(System.getProperty("user.dir")).parentFile

        fun start(limitsJson: String = "{}", port: Int = 0, persist: File? = null): GatewayProcess =
            GatewayProcess(limitsJson, persist).apply { process = spawn(port) }
    }

    private fun spawn(port: Int): Process {
        val builder = ProcessBuilder("node", "--import", "tsx", "src/gateway/main.ts")
            .directory(REPO)
            .redirectErrorStream(false)
        builder.environment()["RTDB_PORT"] = port.toString()
        builder.environment()["RTDB_LIMITS"] = limitsJson
        persist?.let { builder.environment()["RTDB_PERSIST"] = it.absolutePath }
        builder.redirectError(ProcessBuilder.Redirect.INHERIT)

        val process = builder.start()
        val out = BufferedReader(process.inputStream.reader())
        val line = out.readLine() ?: error("gateway exited before listening")
        this.port = Regex("""rtdb listening (\d+)""").find(line)?.groupValues?.get(1)?.toInt()
            ?: error("unexpected gateway output: $line")
        // U3 made stdout a stream the gateway keeps writing to (connection-lifecycle lines). An
        // unread pipe fills at ~64 KiB and then blocks the writer — the gateway would wedge
        // mid-test. Drain it on a daemon thread; the lines themselves are the OS process's business.
        Thread { runCatching { while (out.readLine() != null) Unit } }
            .apply { isDaemon = true }
            .start()
        return process
    }

    /** SIGKILL: no close frames, no graceful shutdown — the process simply stops. */
    fun kill() {
        process?.destroyForcibly()?.waitFor()
        process = null
    }

    fun restart() {
        kill()
        process = spawn(port)
    }

    fun stop() {
        process?.destroy()
        process?.waitFor()
        process = null
    }
}

/**
 * A token the gateway's DevHs256Validator accepts (`RTDB_DEV_SECRET`, default `dev-secret`).
 * Test-only: production tokens come from the app's IdP.
 */
fun devToken(userId: String = "u_kotlin", secret: String = "dev-secret"): String {
    val head = """{"alg":"HS256","typ":"JWT"}""".b64u()
    val claims = """{"sub":"$userId","exp":${System.currentTimeMillis() / 1000 + 3600}}""".b64u()
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
    return "$head.$claims.${mac.doFinal("$head.$claims".toByteArray()).toByteString().base64Url().trimEnd('=')}"
}

private fun String.b64u(): String = toByteArray().toByteString().base64Url().trimEnd('=')
