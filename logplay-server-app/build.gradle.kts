import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    id("kotlin-module-base")
}

repositories {
    mavenCentral()
}

val vertxVersion = "5.0.6"
val junitJupiterVersion = "5.9.1"

dependencies {
    api(project(":logplay-server-domain"))
    api(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
    api("io.vertx:vertx-lang-kotlin-coroutines")
    api("io.vertx:vertx-lang-kotlin")
    implementation("io.vertx:vertx-config")
    implementation("io.vertx:vertx-web")
    implementation("io.vertx:vertx-opentelemetry")
    implementation("io.vertx:vertx-grpc-server")
    testImplementation("io.vertx:vertx-junit5")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events = setOf(PASSED, SKIPPED, FAILED)
    }
}