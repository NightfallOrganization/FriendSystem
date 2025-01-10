enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

pluginManagement {
    repositories {
        gradlePluginPortal()
        maven("https://nexus.darkcube.eu/repository/darkcube-group/") {
            name = "DarkCube"
            credentials(PasswordCredentials::class)
        }
    }
}

plugins {
    id("eu.darkcube.darkcube.settings") version "1.4.0"
}

rootProject.name = "FriendSystem"
