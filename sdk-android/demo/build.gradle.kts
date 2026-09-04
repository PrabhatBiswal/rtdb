import java.util.Properties
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

/**
 * §5.7.2: the shadow key comes from `local.properties`, which `.gitignore` already covers —
 * so the key cannot reach the repo by being typed in the wrong file.
 *
 * A FILE, and never `-PrtdbShadowKey=…`: a Gradle property on the command line is argv, and argv is
 * shell history, `ps` output and every build log that echoes its own invocation. That is the
 * 2026-08-29 SSM lesson (`file://` over an inline value) applied to the build.
 *
 * It still ends up inside the APK, and that is accepted rather than hidden: the phone holding the
 * APK is the same trust boundary as the phone holding a 24h token, this is an internal debug tool
 * installed by hand, and the report says so in as many words. An app shipped to users would put
 * this behind its OWN login and hand out tokens per signed-in user — which is the same TokenSource
 * with a different `fetch` lambda.
 */
val localProperties = Properties().apply {
    rootProject.file("local.properties").takeIf { it.exists() }?.inputStream()?.use { load(it) }
}

/** A Java string literal, escaped — a key with a quote or a backslash must not break the build. */
fun quote(value: String) = "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

// Versions are named here rather than inherited from a parent: this build has no parent any more,
// which is the point (see settings.gradle.kts).
plugins {
    id("com.android.application") version "8.6.1"
    kotlin("android") version "2.1.20"
    // The @Serializable model the R8 check reads back. A compiler plugin, not a dependency — the
    // app applies the same one to its own models at migration (WP3 Gate A ruling Q4).
    kotlin("plugin.serialization") version "2.1.20"
}

android {
    namespace = "com.hobostays.rtdb.demo"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.hobostays.rtdb.demo"
        minSdk = 23
        targetSdk = 35
        versionCode = 1
        versionName = "0.1"
        buildConfigField("String", "RTDB_URL", "\"${project.property("rtdbUrl")}\"")
        buildConfigField("String", "RTDB_TOKEN", "\"${project.findProperty("rtdbToken") ?: ""}\"")
        // §5.7: where the app fetches its own tokens, and the key it presents.
        buildConfigField("String", "SHADOW_TOKEN_URL", "\"${project.property("shadowTokenUrl")}\"")
        buildConfigField("String", "SHADOW_KEY", quote(localProperties.getProperty("rtdbShadowKey").orEmpty()))
        // §5.7.4's live hook: seconds after which the app PRETENDS its token expires, so the
        // refresh can be watched on screen in a minute instead of in a day. 0 = trust exp.
        buildConfigField(
            "long",
            "TOKEN_LIFETIME_SECONDS",
            "${(project.findProperty("tokenLifetimeSeconds") as String? ?: "0").toLong()}L",
        )
    }

    buildTypes {
        // WORKLOAD §2's R8 check. The release build is the ONLY one whose answer means anything:
        // a debug build never runs a shrinker, so `getValue(Class)` passing there proves nothing at
        // all about the APK a migrating app would actually ship.
        release {
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            // Signed with the DEBUG key, so the minified build can actually be installed on the
            // drill phone. Without it `assembleRelease` produces an unsigned APK and the R8 proof
            // stays theoretical — the one build whose behaviour is in question would be the one
            // build nobody can run. This is a demo app that ships to nobody; a real release key is
            // the app's own business, not the SDK's.
            signingConfig = signingConfigs.getByName("debug")
        }
    }

    buildFeatures {
        buildConfig = true
        viewBinding = true
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

kotlin {
    compilerOptions { jvmTarget = JvmTarget.JVM_17 }
}

dependencies {
    // WORKLOAD §2's dependency proof, and the snippet an integrating engineer pastes. No project(...),
    // no includeBuild, no source: this resolves from ~/.m2 and nothing else can satisfy it. The core
    // (com.hobostays.rtdb:rtdb-kotlin-core:0.1.0) arrives transitively at `compile` scope, along
    // with kotlinx-serialization and kotlinx-coroutines — which is what `api` bought (WP3-A Q3).
    implementation("com.hobostays.rtdb:rtdb-android:0.1.0")
    implementation("androidx.appcompat:appcompat:1.7.0")
    // The ONE approved addition (WORKLOAD §5.5). Material carries the cards, buttons and the dark
    // Material3 theme; RecyclerView carries the child list. Nothing else is added.
    implementation("com.google.android.material:material:1.12.0")
    implementation("androidx.recyclerview:recyclerview:1.3.2")
    // §5.7.4 asks for named JVM tests, which needs a runner. Test-only: it is not on the app's
    // compile classpath and never reaches the APK. The one addition this package makes.
    testImplementation("junit:junit:4.13.2")
}

/**
 * WORKLOAD §2's R8 check, as a task rather than a promise (WP3 Gate C Q3).
 *
 * `getValue(Class<T>)` finds a serializer at RUNTIME, by reflection, walking exactly three symbols:
 * the `Companion` FIELD on the model, the `serializer()` METHOD on that companion, and the generated
 * `$$serializer` class it returns. Nothing in the SDK or the app names any of them statically, so a
 * shrinker is free to delete or rename all three — and if it renames them the lookup fails at
 * runtime with no compile error and no build warning, on release builds only. That is the worst
 * shape a bug can have, so it gets a check that reads R8's OWN report rather than a rule we hope
 * is enough.
 *
 * The answer, as of AGP 8.6.1 / kotlinx-serialization 1.8.1: no rule of ours is needed. The two
 * files kotlinx ships in its own artifact (`kotlinx-serialization-common.pro`,
 * `kotlinx-serialization-r8.pro`) keep all three by name, and they reach R8 automatically because
 * the core declares kotlinx as `api`. `proguard-rules.pro` is empty and stays empty — this task is
 * what will say so if that ever stops being true.
 */
val verifyR8KeepsSerializers = tasks.register("verifyR8KeepsSerializers") {
    description = "Proves the generated serializer survives R8 with the name getValue(Class) looks it up by."
    group = "verification"
    val mapping = layout.buildDirectory.file("outputs/mapping/release/mapping.txt")
    inputs.file(mapping).withPropertyName("mapping")
    doLast {
        val model = "com.hobostays.rtdb.demo.DemoMember"
        val text = mapping.get().asFile.readText()

        // The lookup is by NAME, so "kept" is not enough — each has to survive UNRENAMED.
        // A mapping line reads `original -> obfuscated`; `x -> x` is the identity we need.
        // In a Kotlin string a bare `$` starts a template, so the literal dollars in
        // `Model$Companion` and `Model$$serializer` are escaped rather than raw-stringed.
        val required = listOf(
            "the model's Companion field" to
                Regex("\\s\\Q$model\\E\\\$Companion Companion -> Companion$", RegexOption.MULTILINE),
            "the companion's serializer() method" to
                Regex("kotlinx\\.serialization\\.KSerializer serializer\\(\\).* -> serializer$", RegexOption.MULTILINE),
            "the generated \$\$serializer class" to
                Regex("^\\Q$model\\E\\\$\\\$serializer ->", RegexOption.MULTILINE),
        )
        val missing = required.filterNot { (_, re) -> re.containsMatchIn(text) }.map { it.first }
        if (missing.isNotEmpty()) {
            throw GradleException(
                "R8 did not preserve what getValue(Class) looks up on $model: ${missing.joinToString()}.\n" +
                    "A minified build will throw \"cannot read …: annotate it @Serializable\" at runtime " +
                    "while the debug build passes. Check that the model is still @Serializable and that " +
                    "kotlinx-serialization's consumer rules still reach R8 " +
                    "(build/outputs/mapping/release/configuration.txt lists every rule that was applied).",
            )
        }
        logger.lifecycle("R8 kept the lookup chain on $model: Companion, serializer(), \$\$serializer — all unrenamed.")
    }
}

// A minified APK cannot be produced without the check running over it. Wiring it to `check` instead
// would leave the release build — the only one where this can break — able to pass unexamined.
tasks.matching { it.name == "assembleRelease" }.configureEach { finalizedBy(verifyR8KeepsSerializers) }
