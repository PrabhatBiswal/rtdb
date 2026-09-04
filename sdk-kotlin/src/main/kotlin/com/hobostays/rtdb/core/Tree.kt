package com.hobostays.rtdb.core

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/** The stored unit: a flattened leaf path and its value. */
data class Leaf(val path: String, val value: JsonElement)

/**
 * Flatten a value into leaves (§1), the port of WP1's `src/storage/tree.ts`.
 *  - `null` and `{}` produce NO leaves — both mean "nothing is stored here";
 *  - arrays are opaque leaf values, Firebase-style.
 *
 * Unlike the server's, this one does not validate keys or count leaves: §9's caps are the gateway's
 * to enforce (it answers TOOBIG), and the write API validates the leaf paths it is about to send.
 */
fun flatten(path: String, value: JsonElement): List<Leaf> {
    val leaves = mutableListOf<Leaf>()

    fun walk(at: String, node: JsonElement) {
        when {
            node is JsonObject -> for ((key, child) in node) walk(joinPath(at, key), child)
            node is JsonNull -> Unit // a null anywhere in the value means "no leaf here"
            else -> leaves += Leaf(at, node)
        }
    }

    walk(path, value)
    return leaves
}

/** Rebuild the value rooted at `root` from its leaves. No leaves -> null (§1). */
fun unflatten(root: String, leaves: List<Leaf>): JsonElement {
    if (leaves.isEmpty()) return JsonNull
    // A leaf sitting exactly at the root means the root itself is a scalar or an array.
    leaves.firstOrNull { it.path == root }?.let { return it.value }

    val tree = LinkedHashMap<String, Any>()
    for (leaf in leaves) {
        val segs = segments(relativePath(root, leaf.path))
        var node = tree
        for (i in 0 until segs.size - 1) {
            val existing = node[segs[i]]
            @Suppress("UNCHECKED_CAST")
            node = if (existing is LinkedHashMap<*, *>) {
                existing as LinkedHashMap<String, Any>
            } else {
                LinkedHashMap<String, Any>().also { node[segs[i]] = it }
            }
        }
        node[segs.last()] = leaf.value
    }
    return toJson(tree)
}

private fun toJson(node: Any): JsonElement = when (node) {
    is LinkedHashMap<*, *> -> JsonObject(node.entries.associate { (k, v) -> k as String to toJson(v!!) })
    else -> node as JsonElement
}
