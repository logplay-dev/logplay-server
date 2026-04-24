import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    id("kotlin-module-base")
    application
    alias(libs.plugins.shadow)
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("dev.logplay.server.h2.MainKt")
}

dependencies {
    implementation(project(":logplay-server-app"))
    implementation(libs.h2.database)
    implementation(libs.flyway.core)
    implementation(libs.log4j.slf4j2.impl)

    testImplementation(testFixtures(project(":logplay-server-app")))
    testImplementation(platform(libs.vertx.bom))
    testImplementation(libs.vertx.web.client)
    testImplementation(libs.vertx.junit5)
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.kotlinx.coroutines.test)
}

tasks.withType<ShadowJar> {
    archiveClassifier.set("fat")
    mergeServiceFiles()
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events = setOf(PASSED, SKIPPED, FAILED)
    }
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    testLogging {
        events = setOf(PASSED, SKIPPED, FAILED)
    }
    shouldRunAfter(tasks.test)
}