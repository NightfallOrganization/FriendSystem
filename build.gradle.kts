import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("eu.darkcube.darkcube")
    alias(libs.plugins.shadow)
    java
}

val include = configurations.register("include")

dependencies {
    compileOnly(libs.velocity.api)
    annotationProcessor(libs.velocity.api)
    implementation(libs.hikaricp)
    implementation(libs.mariadb.java.client)
    implementation(libs.slf4j.simple)

    include(libs.hikaricp)
    include(libs.mariadb.java.client)
}

tasks {
    jar {
        destinationDirectory = temporaryDir
    }
    shadowJar {
        configurations = listOf(include.get())
        crelocate("com.github.benmanes.caffeine", "caffeine")
        crelocate("com.google.errorprone", "errorprone")
        crelocate("com.sun.jna", "jna")
        crelocate("com.zaxxer.hikari", "hikari")
        crelocate("org.apache.commons.logging", "apache.commons.logging")
        crelocate("org.checkerframework", "checkerframework")
        crelocate("org.mariadb", "mariadb")
        crelocate("waffle", "waffle")
        exclude("org/slf4j/**")
        archiveClassifier = null
    }
    assemble {
        dependsOn(shadowJar)
    }
}

fun ShadowJar.crelocate(origin: String, dest: String) {
    this.relocate(origin, "eu.darkcube.friendsystem.velocity.libs.$dest")
}