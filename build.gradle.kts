import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("org.springframework.boot") version "3.3.0"
    id("io.spring.dependency-management") version "1.1.4"
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.spring") version "2.2.21"
}

group = "com.scalpsecta"
version = "0.1.0-SNAPSHOT"

val linerDtoJar = providers.gradleProperty("linerDtoJar")
    .orElse(providers.environmentVariable("LINER_DTO_JAR"))
    .orElse("C:/Users/Иван/IdeaProjects/liner-dto/build/libs/liner-dto-1.0.0.jar")
    .map { file(it) }
    .get()
val linerStarterJar = providers.gradleProperty("linerStarterJar")
    .orElse(providers.environmentVariable("LINER_STARTER_JAR"))
    .orElse("C:/IdeaProjects/liner-starter/build/libs/liner-spring-boot-starter-1.1.0.jar")
    .map { file(it) }
    .get()

require(linerDtoJar.isFile) {
    "liner-dto JAR not found at $linerDtoJar; set linerDtoJar or LINER_DTO_JAR"
}
require(linerStarterJar.isFile) {
    "liner-starter JAR not found at $linerStarterJar; set linerStarterJar or LINER_STARTER_JAR"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation(files(linerDtoJar, linerStarterJar))
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    testImplementation("org.springframework.boot:spring-boot-test")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testImplementation("io.projectreactor:reactor-test")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
