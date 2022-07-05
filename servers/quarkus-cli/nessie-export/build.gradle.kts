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
  implementation(projects.model)
  implementation("com.google.code.gson:gson:2.8.9")
  implementation("org.apache.commons:commons-lang3:3.12.0")
}

