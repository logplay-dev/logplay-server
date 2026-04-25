plugins {
    `java-library`
    id("com.diffplug.spotless")

}

group = "org.zeplinko.logplay"
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
