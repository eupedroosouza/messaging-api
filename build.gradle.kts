plugins {
    java
    `java-library`
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

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("com.github.fppt:jedis-mock:1.0.11")
}

java {
    withSourcesJar()

    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    withType<JavaCompile> {
        options.release = 8
        options.encoding = "UTF-8"
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