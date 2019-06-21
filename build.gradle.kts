/*
 * **************************************************************************
 * Copyright 2014-2019 Spectra Logic Corporation. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 * this file except in compliance with the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * **************************************************************************
 */

plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.21")
    application
    maven
}

repositories {
    jcenter()
    mavenLocal()
    mavenCentral()
}

dependencies {
    val cliktVersion = "2.0.0"
    val mockkVersion = "1.9.3"

    val junitVersion = "3.3.2"
    val kotlinCoroutinesVersion = "1.3.0-M1"

    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    compile("org.jetbrains.kotlin:kotlin-reflect:1.3.31")
    implementation("com.github.ajalt:clikt:$cliktVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("com.spectralogic.retrofitutils:retrofit-utils:1.0.6-dev")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:$junitVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform { }
}


application {
    mainClassName = "com.spectralogic.rioBench.AppKt"
}

