import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    id("kotlin-module-base")
    `java-test-fixtures`
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":logplay-server-domain"))
    api(platform(libs.vertx.bom))
    api(libs.vertx.lang.kotlin.coroutines)
    api(libs.vertx.lang.kotlin)
    implementation(libs.vertx.config)
    implementation(libs.vertx.web)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.vertx.opentelemetry)
    implementation(libs.vertx.grpc.server)
    implementation(platform(libs.opentelemetry.bom))
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.sdk.autoconfigure)
    implementation(libs.log4j.slf4j2.impl)
    testImplementation(libs.vertx.junit5)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)

    testFixturesImplementation(platform(libs.vertx.bom))
    testFixturesImplementation(libs.vertx.web.client)
    testFixturesImplementation(libs.vertx.junit5)
    testFixturesImplementation(libs.junit.jupiter)
    testFixturesRuntimeOnly(libs.junit.platform.launcher)
    testFixturesImplementation(libs.assertj.core)
    testFixturesImplementation(libs.kotlinx.coroutines.test)
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events = setOf(PASSED, SKIPPED, FAILED)
    }
}