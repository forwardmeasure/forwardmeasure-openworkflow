/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.openworkflow.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

class PortableBoundaryArchitectureTest {
  private static final String PRODUCT_PACKAGES = "com.forwardmeasure.openworkflow..";

  @Test
  void portableWp1TypesDoNotDependOnFrameworksPersistenceOrEngines() {
    var classes = new ClassFileImporter().importPackages("com.forwardmeasure.openworkflow");

    noClasses()
        .that()
        .resideInAnyPackage("..model..", "..definition..", "..expression..", "..engine.api..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "io.quarkus..",
            "org.springframework..",
            "io.micronaut..",
            "jakarta.persistence..",
            "jakarta.ws.rs..",
            "org.hibernate..",
            "org.apache.kafka..",
            "org.apache.pekko..",
            "java.net.http..")
        .allowEmptyShould(true)
        .check(classes);
  }

  @Test
  void authorizationApiIsPortableAndHasNoHttpOrFrameworkDependency() {
    var classes = new ClassFileImporter().importPackages("com.forwardmeasure.openworkflow");

    noClasses()
        .that()
        .resideInAnyPackage("..authorization")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "java.net.http..",
            "io.quarkus..",
            "org.springframework..",
            "io.micronaut..",
            "jakarta.persistence..",
            "org.hibernate..")
        .allowEmptyShould(true)
        .check(classes);
  }

  @Test
  void specificationAndCompilerDoNotDependOnTheEngineContract() {
    var classes = new ClassFileImporter().importPackages("com.forwardmeasure.openworkflow");

    noClasses()
        .that()
        .resideInAnyPackage("..model..", "..definition..", "..expression..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("..engine.api..")
        .allowEmptyShould(true)
        .check(classes);
  }

  @Test
  void engineContractDoesNotDependOnProductAdapters() {
    var classes = new ClassFileImporter().importPackages("com.forwardmeasure.openworkflow");

    noClasses()
        .that()
        .resideInAnyPackage("..engine.api..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "..persistence..", "..jaxrs..", "..http..", "..engine.pekko..", "..engine.kafka..")
        .allowEmptyShould(true)
        .check(classes);
  }

  @Test
  void productTypesStayInTheProductNamespace() {
    var classes = new ClassFileImporter().importPackages("com.forwardmeasure.openworkflow");

    noClasses()
        .that()
        .resideOutsideOfPackage(PRODUCT_PACKAGES)
        .should()
        .resideInAnyPackage(PRODUCT_PACKAGES)
        .allowEmptyShould(true)
        .check(classes);
  }
}
