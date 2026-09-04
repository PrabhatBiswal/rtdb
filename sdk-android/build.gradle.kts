// Versions live here rather than in a version catalog: two modules do not need the indirection.
plugins {
    id("com.android.library") version "8.6.1" apply false
    id("com.android.application") version "8.6.1" apply false
    // WORKLOAD §3: Kotlin 2.1.20, LOCKED at WP2 Gate A. Must match sdk-kotlin.
    kotlin("android") version "2.1.20" apply false
    // For the @Serializable model in :library's tests — ruling Q4's generated serializers are a
    // compiler plugin, not a dependency. The app applies it to its own models at migration.
    kotlin("plugin.serialization") version "2.1.20" apply false
}

/**
 * The pair, in one command — what the demo and, later, any consuming app actually need in `~/.m2`:
 *
 *     ./gradlew publishSdkToMavenLocal
 *
 * A composite build does not propagate lifecycle tasks into included builds, so `publishToMavenLocal`
 * run here would publish the AAR and quietly leave the core it depends on unpublished — a consumer
 * would then fail to resolve `com.hobostays.rtdb:rtdb-kotlin-core:0.1.0` and the cause would look
 * like a repository problem rather than a missing publish. The dependency is declared instead.
 *
 * mavenLocal is the ONLY target (user ruling 2026-08-30); a remote repository is a later package.
 */
tasks.register("publishSdkToMavenLocal") {
    group = "publishing"
    description = "Publishes com.hobostays.rtdb:rtdb-kotlin-core and com.hobostays.rtdb:rtdb-android to ~/.m2."
    // By the included BUILD, whose name comes from its directory — not from its rootProject.name,
    // and not from the artifact id. Taking them all keeps this correct if another is ever added.
    dependsOn(gradle.includedBuilds.map { it.task(":publishToMavenLocal") })
    dependsOn(":library:publishToMavenLocal")
}
