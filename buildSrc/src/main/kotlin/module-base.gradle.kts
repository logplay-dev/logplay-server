plugins {
    `java-library`
    id("com.diffplug.spotless")

}

group = "dev.logplay"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
}

spotless {
    kotlin {
        ktfmt().kotlinlangStyle()
    }
}
