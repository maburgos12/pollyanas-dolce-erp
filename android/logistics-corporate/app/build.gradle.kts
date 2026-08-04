plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("com.google.devtools.ksp")
}

val releaseStorePath = providers.environmentVariable("POLLYANAS_KEYSTORE_PATH").orNull
val releaseStorePassword = providers.environmentVariable("POLLYANAS_KEYSTORE_PASSWORD").orNull
val releaseKeyAlias = providers.environmentVariable("POLLYANAS_KEY_ALIAS").orNull
val releaseKeyPassword = providers.environmentVariable("POLLYANAS_KEY_PASSWORD").orNull
val releaseSigningAvailable = listOf(
    releaseStorePath,
    releaseStorePassword,
    releaseKeyAlias,
    releaseKeyPassword,
).all { !it.isNullOrBlank() }

android {
    namespace = "com.pollyanas.logistics"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.pollyanas.logistics.corporate"
        minSdk = 29
        targetSdk = 35
        versionCode = 1
        versionName = "1.0.1"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        buildConfigField("String", "ERP_ORIGIN", "\"https://erp.pollyanasdolce.com\"")
    }
    buildFeatures { buildConfig = true }
    signingConfigs {
        create("corporateRelease") {
            if (releaseSigningAvailable) {
                storeFile = file(requireNotNull(releaseStorePath))
                storePassword = releaseStorePassword
                keyAlias = releaseKeyAlias
                keyPassword = releaseKeyPassword
                storeType = "PKCS12"
                enableV1Signing = true
                enableV2Signing = true
            }
        }
    }
    buildTypes {
        release {
            isMinifyEnabled = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            if (releaseSigningAvailable) {
                signingConfig = signingConfigs.getByName("corporateRelease")
            }
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}

tasks.matching { it.name == "bundleRelease" || it.name == "assembleRelease" }.configureEach {
    doFirst {
        check(releaseSigningAvailable) {
            "Release signing requires POLLYANAS_KEYSTORE_PATH, POLLYANAS_KEYSTORE_PASSWORD, " +
                "POLLYANAS_KEY_ALIAS and POLLYANAS_KEY_PASSWORD."
        }
    }
}

dependencies {
    implementation("androidx.core:core-ktx:1.15.0")
    implementation("androidx.appcompat:appcompat:1.7.1")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.7")
    implementation("androidx.lifecycle:lifecycle-service:2.8.7")
    implementation("androidx.room:room-runtime:2.6.1")
    implementation("androidx.room:room-ktx:2.6.1")
    ksp("androidx.room:room-compiler:2.6.1")
    implementation("androidx.security:security-crypto:1.1.0")
    implementation("androidx.sqlite:sqlite-ktx:2.6.2")
    implementation("net.zetetic:sqlcipher-android:4.15.0@aar")
    implementation("com.google.android.gms:play-services-location:21.3.0")
    testImplementation("junit:junit:4.13.2")
}
