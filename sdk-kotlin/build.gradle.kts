import java.util.zip.ZipFile
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    // `api` (below) needs java-library; the Kotlin JVM plugin only brings plain `java`.
    `java-library`
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization") version "2.1.20"
    // WORKLOAD §3/§4: Gradle's built-in publisher. It is not a dependency and nothing else is added.
    `maven-publish`
}

// WORKLOAD §2: a consuming app must take this as a VERSIONED ARTIFACT, not as source modules in our
// repo, so the coordinate is the deliverable, and the standing user order about which name the
// project carries applies to it first of all. The artifact id says what it is — the plain-JVM core,
// the thing sdk-android wraps.
group = "com.hobostays.rtdb"
version = "0.1.0"

repositories { mavenCentral() }

// WORKLOAD §3: JVM target 17, but the core must run on Android minSdk 23 — no java.time, no Java 8+
// APIs that need desugaring. Built with whatever JDK >= 17 is present rather than a toolchain, so
// the build needs no auto-provisioning (and no extra plugin) on a dev machine or in CI.
kotlin {
    compilerOptions { jvmTarget = JvmTarget.JVM_17 }
}
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    // A binary with no sources is a black box in a debugger, and this SDK's whole job is to be
    // stepped through when a migration behaves oddly. Javadoc jar deliberately absent: it would be
    // empty of anything KDoc-aware tooling could not already read from the sources.
    withSourcesJar()
}

dependencies {
    // OkHttp is a true implementation detail: nothing a consumer has to name to call this SDK.
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    // `api`, not `implementation` (WP3 Gate A ruling Q3): both are on this library's PUBLIC surface
    // — DataSnapshot.value/setValue are JsonElement, RtdbClient takes a CoroutineDispatcher and
    // exposes a StateFlow — and a library must not hide the types it makes callers use.
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging { events("failed") }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            // `components["java"]` carries the dependency SCOPES the java-library plugin computed:
            // `api` -> POM `compile`, `implementation` -> POM `runtime`. That distinction is the
            // whole reason this module uses java-library (WP3 Gate A ruling Q3), and it only
            // survives into the POM because it is published from the component rather than by hand.
            from(components["java"])
            artifactId = "rtdb-kotlin-core"
            pom {
                name = "RTDB Kotlin core"
                description =
                    "Plain-JVM client for the RTDB wire protocol (PROTOCOL.md v1.5): connection FSM, " +
                        "subscriptions, the two-layer mirror, and the Firebase-shaped surface."
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
            }
        }
    }
    // WORKLOAD §2 (user ruling 2026-08-30): mavenLocal ONLY. A remote repository is a later work
    // package, scheduled when a real migration actually needs one. Declaring none leaves
    // `publishToMavenLocal` as the only publish task, which is the honest way to say that.
}

/**
 * The sources jar is a SHIPPED artifact, and it is assembled from the FILESYSTEM rather than from
 * what git tracks. That difference had already cost something by the time this check was written:
 * two empty package directories left behind by the WP2 rename were invisible to git (it does not
 * track empty directories) and to six work packages of review, and went straight into the jar the
 * moment one was built. An artifact must contain the project and nothing else.
 */
val verifySourcesJarContents = tasks.register("verifySourcesJarContents") {
    description = "Fails if the published sources jar contains anything outside com/hobostays."
    group = "verification"
    val jar = tasks.named<Jar>("sourcesJar").flatMap { it.archiveFile }
    inputs.file(jar).withPropertyName("sourcesJar")
    doLast {
        // Read the archive's own entry names rather than a FileTree: a Gradle file tree has no
        // files for an EMPTY directory entry, and an empty directory is exactly what leaked here.
        val strays = ZipFile(jar.get().asFile).use { zip ->
            zip.entries().asSequence().map { it.name }
                .filterNot { it.startsWith("com/hobostays/") || it.startsWith("META-INF/") || it == "com/" }
                .toList()
        }
        if (strays.isNotEmpty()) {
            throw GradleException(
                "the sources jar carries entries that are not this project: $strays. Empty directories " +
                    "left over from a package rename are the usual cause — git cannot see them, so only " +
                    "building the artifact can.",
            )
        }
        logger.lifecycle("sources jar: com/hobostays only, no strays.")
    }
}
tasks.named("sourcesJar") { finalizedBy(verifySourcesJarContents) }
