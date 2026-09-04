package com.hobostays.rtdb.core

import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.jupiter.api.Test

/** §1 path rules, ported from WP1's `test/unit/path.test.ts` (including the v1.1 control chars). */
class PathsTest {

    @Test
    fun `root and ordinary paths are valid`() {
        assertNull(validatePath(""))
        assertNull(validatePath("MPK_1010"))
        assertNull(validatePath("MPK_1010/1474396/score"))
        assertNull(validatePath("a-b_c~d!e"))
    }

    @Test
    fun `leading, trailing and doubled slashes are empty segments`() {
        for (path in listOf("/a", "a/", "a//b", "/", "//")) {
            assertEquals("empty path segment", validatePath(path), "for \"$path\"")
        }
    }

    @Test
    fun `the forbidden characters are rejected, one at a time`() {
        for (char in listOf('.', '#', '$', '[', ']')) {
            assertNotNull(validatePath("a${char}b"), "\"$char\" must be rejected in a segment")
        }
    }

    @Test
    fun `control characters are rejected too (v1_1)`() {
        for (code in listOf(0x00, 0x01, 0x09, 0x0a, 0x1f, 0x7f)) {
            assertNotNull(
                validatePath("a" + code.toChar() + "b"),
                "U+%04X must be rejected in a segment".format(code),
            )
        }
        assertNull(validatePath("a\u0080b"), "U+0080 is outside the forbidden ranges")
    }

    @Test
    fun `depth is capped at 32 segments`() {
        assertNull(validatePath(List(32) { "s" }.joinToString("/")))
        assertEquals("path deeper than 32 segments", validatePath(List(33) { "s" }.joinToString("/")))
    }

    @Test
    fun `length is capped at 768 BYTES, not characters`() {
        assertNull(validatePath("a".repeat(768)))
        assertEquals("path longer than 768 bytes", validatePath("a".repeat(769)))
        // "अ" is 3 UTF-8 bytes: 256 of them are exactly the cap, 257 are over it.
        assertNull(validatePath("अ".repeat(256)))
        assertNotNull(validatePath("अ".repeat(257)))
    }

    @Test
    fun `the limits are overridable for tests`() {
        val tiny = Limits(maxPathDepth = 2, maxPathBytes = 8)
        assertNull(validatePath("a/b", tiny))
        assertNotNull(validatePath("a/b/c", tiny))
        assertNotNull(validatePath("aaaaaaaaa", tiny))
    }
}
