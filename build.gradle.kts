import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    `java-library`
    kotlin("jvm") version "1.9.22"
    `maven-publish`
    id("org.cadixdev.licenser") version "0.6.1"
}

group = "com.github.eupedroosouza"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {

    api(libs.jedis)
    api(libs.gson)

    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(8)
}

java {
    withSourcesJar()

    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    withType<JavaCompile> {
        options.release = 8
    }

    withType<KotlinCompile> {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }

    test {
        useJUnitPlatform()
        testLogging {
            events("failed")
            setExceptionFormat("full")
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }

        repositories {
            // Repository here
        }
    }
}