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
package com.forwardmeasure.openworkflow.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class KeycloakOrganizationClaimsTest {
  private static final String ID = "11111111-1111-1111-1111-111111111111";
  private static final String ORGANIZATION_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

  @Test
  void extractsOnlyRolesNestedUnderTheActiveOrganization() {
    var claims =
        Map.<String, Object>of(
            "sub", "actor-1",
            "resource_access",
                Map.of("forwardmeasure-openworkflow", Map.of("roles", Set.of("leaked"))),
            "organization",
                Map.of(
                    "tenant-a",
                    Map.of(
                        "id",
                        ORGANIZATION_ID,
                        "forwardmeasure.tenant-id",
                        Set.of(ID),
                        "resource_access",
                        Map.of(
                            "forwardmeasure-openworkflow",
                            Map.of("roles", Set.of("workflow-author"))))));

    ActiveOrganization active =
        KeycloakOrganizationClaims.extract(claims, "forwardmeasure-openworkflow");

    assertEquals(ORGANIZATION_ID, active.organizationId());
    assertEquals(ID, active.tenantId().toString());
    assertEquals(Set.of("workflow-author"), active.organizationRoles());
  }

  @Test
  void rejectsAmbiguousOrMissingActiveOrganizations() {
    assertThrows(
        SecurityException.class,
        () ->
            KeycloakOrganizationClaims.extract(
                Map.of("sub", "actor", "organization", Map.of()), "forwardmeasure-openworkflow"));
    assertThrows(
        SecurityException.class,
        () ->
            KeycloakOrganizationClaims.extract(
                Map.of("sub", "actor", "organization", Map.of("a", Map.of(), "b", Map.of())),
                "forwardmeasure-openworkflow"));
  }
}
