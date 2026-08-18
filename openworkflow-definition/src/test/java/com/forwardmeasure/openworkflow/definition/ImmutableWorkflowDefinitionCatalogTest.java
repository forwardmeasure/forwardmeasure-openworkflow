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
package com.forwardmeasure.openworkflow.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class ImmutableWorkflowDefinitionCatalogTest {
  private static final String SOURCE_SHA = "a".repeat(64);
  private static final String DEFINITION_SHA = "b".repeat(64);

  @Test
  void resolvesExactAndPinsLatestUsingSemverPrecedence() {
    var versionOne = subflow("1.0.0");
    var prerelease = subflow("2.0.0-rc.1");
    var versionTwo = subflow("2.0.0");
    var catalog =
        new ImmutableWorkflowDefinitionCatalog(List.of(versionOne, prerelease, versionTwo));

    assertEquals(
        versionOne, catalog.resolve("entity-intelligence", "activation", "1.0.0").orElseThrow());
    assertEquals(
        versionTwo, catalog.resolve("entity-intelligence", "activation", "latest").orElseThrow());
  }

  @Test
  void rejectsConflictingImmutableCoordinates() {
    var original = subflow("1.0.0");
    var conflicting = new ResolvedSubflow(original.coordinates(), "c".repeat(64), DEFINITION_SHA);

    assertThrows(
        IllegalArgumentException.class,
        () -> new ImmutableWorkflowDefinitionCatalog(List.of(original, conflicting)));
  }

  private static ResolvedSubflow subflow(String version) {
    return new ResolvedSubflow(
        new WorkflowCoordinates("entity-intelligence", "activation", version, "1.0.3"),
        SOURCE_SHA,
        DEFINITION_SHA);
  }
}
