package com.hobostays.rtdb.demo

import kotlinx.serialization.Serializable

/**
 * The R8 check's subject (WORKLOAD §2, recorded at WP3 Gate C Q3).
 *
 * `getValue(Class<T>)` does NOT reflect over fields — ruling Q4 — it looks up the serializer
 * kotlinx-serialization GENERATED for this class, at runtime, from the `Class` object the caller
 * passed. Nothing in the SDK names `DemoMember$$serializer` or `DemoMember.Companion` statically, so
 * a shrinker has no static reference to follow and every reason to delete both. This is the one
 * shape in the whole SDK whose correctness depends on a ProGuard rule rather than on code, which is
 * exactly why it gets a minified build of its own instead of a promise.
 *
 * A migrating app's models become `@Serializable` at migration; this stands in for all of them.
 */
@Serializable
data class DemoMember(val name: String, val score: Int)
