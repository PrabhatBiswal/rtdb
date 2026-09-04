package com.hobostays.rtdb.android

import android.os.Handler
import android.os.Looper
import java.util.concurrent.Executor

/**
 * PROTOCOL §7 threading, the Android half: "callbacks on main thread; I/O + mirror on a background
 * dispatcher". This is the whole of it — WP2 left `RtdbClient(callbackExecutor = ...)` as the seam,
 * so Android contributes an Executor and nothing else changes.
 *
 * A `Handler` on the main Looper rather than `Context.getMainExecutor()`, which is API 28 and
 * minSdk is 23 (WORKLOAD §2).
 *
 * It posts unconditionally, including from the main thread itself. Running inline when already on
 * the main thread would be faster and WRONG: [com.hobostays.rtdb.api.RtdbClient] requires this
 * executor to be order-preserving, and an inline call would jump ahead of callbacks already sitting
 * in the queue.
 */
object MainThreadExecutor : Executor {
    private val handler = Handler(Looper.getMainLooper())

    override fun execute(command: Runnable) {
        handler.post(command)
    }
}
