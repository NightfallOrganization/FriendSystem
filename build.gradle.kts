plugins {
    id("eu.darkcube.darkcube")
    `java-library`
}

dependencies {
    compileOnly(libs.velocity.api)
    annotationProcessor(libs.velocity.api)
}