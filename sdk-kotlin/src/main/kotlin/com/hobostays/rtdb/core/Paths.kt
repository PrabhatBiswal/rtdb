package com.hobostays.rtdb.core

/** §1 (v1.1): segments must not contain / . # $ [ ] nor control chars U+0000–U+001F, U+007F. */
private val FORBIDDEN = Regex("""[./#${'$'}\[\]\u0000-\u001F\u007F]""")

/**
 * §1 path validation. Returns null when valid, else a human reason (the caller maps it to BADPATH).
 * Root is "" (depth 0). No leading/trailing/double slash — those show up as empty segments.
 *
 * The SDK validates locally so a malformed path fails at the call site instead of costing a round
 * trip; the gateway validates the same rules again, because a client is never the authority.
 */
fun validatePath(path: String, limits: Limits = Limits()): String? {
    if (path.isEmpty()) return null
    if (path.toByteArray(Charsets.UTF_8).size > limits.maxPathBytes) {
        return "path longer than ${limits.maxPathBytes} bytes"
    }
    val segments = path.split('/')
    if (segments.size > limits.maxPathDepth) return "path deeper than ${limits.maxPathDepth} segments"
    for (segment in segments) {
        if (segment.isEmpty()) return "empty path segment"
        if (FORBIDDEN.containsMatchIn(segment)) {
            return "path segment contains one of / . # $ [ ] or a control character"
        }
    }
    return null
}

/** Segments of a path; root ("") has none. */
fun segments(path: String): List<String> = if (path.isEmpty()) emptyList() else path.split('/')

/** Join an absolute base with a relative path (either may be ""). */
fun joinPath(base: String, relative: String): String = when {
    base.isEmpty() -> relative
    relative.isEmpty() -> base
    else -> "$base/$relative"
}

/** Is `a` the same path as `b`, or an ancestor of it? Root ("") is an ancestor of everything. */
fun isAncestorOrEqual(a: String, b: String): Boolean = a.isEmpty() || a == b || b.startsWith("$a/")

/**
 * §3 relevance: one path is at-or-under the other. Used identically by delta routing and by the
 * event fan-out — one predicate, every call site.
 */
fun isRelevant(a: String, b: String): Boolean = isAncestorOrEqual(a, b) || isAncestorOrEqual(b, a)

/** `path` and all its ancestors, root first (at most 33 entries). */
fun ancestorsInclusive(path: String): List<String> {
    val out = mutableListOf("")
    val segs = segments(path)
    for (i in segs.indices) out += segs.subList(0, i + 1).joinToString("/")
    return out
}

/** `path` expressed relative to `ancestor` ("" when they are equal). Assumes [isAncestorOrEqual]. */
fun relativePath(ancestor: String, path: String): String =
    if (ancestor.isEmpty()) path else path.substring(ancestor.length + 1)
