/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
  `java-library`
  jacoco
  `maven-publish`
  `nessie-conventions`
  id("org.projectnessie.buildsupport.attach-test-jar")
}

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(projects.servers.quarkusCommon)
  implementation(projects.servers.services)
  implementation(projects.servers.store)
  implementation(projects.versioned.persist.adapter)
  implementation(projects.versioned.persist.serialize)
  implementation(projects.versioned.persist.serializeProto)
  implementation(projects.versioned.spi)
  implementation(projects.versioned.persist.persistStore)
  implementation(projects.versioned.persist.mongodb)
  implementation(projects.versioned.persist.dynamodb)
  implementation(projects.versioned.persist.rocks)
  implementation(projects.versioned.persist.nontx)
  implementation(projects.versioned.persist.tx)
  implementation(projects.model)
  implementation("com.google.code.gson:gson:2.8.9")
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("com.fasterxml.jackson.core:jackson-core:2.13.3")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3")
  implementation("software.amazon.awssdk:auth:2.17.220")
  implementation("software.amazon.awssdk:dynamodb:2.17.220")
  implementation("software.amazon.awssdk:aws-crt-client:2.17.220-PREVIEW")

  testImplementation(platform(rootProject))
  testImplementation(projects.servers.quarkusCommon)
  testImplementation("junit:junit:4.13.2")
  testImplementation("org.assertj:assertj-core:3.21.0")
  testImplementation("software.amazon.awssdk:apache-client:2.17.220")

}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}
