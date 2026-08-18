/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.tenant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class CapabilityPackPolicyTest {
  @Test
  void versionedPolicyContainsTheLockedRoleAndActionMatrix() throws IOException {
    JsonNode root =
        new ObjectMapper()
            .readTree(
                CapabilityPackPolicyTest.class.getResourceAsStream(
                    "/openworkflow-capability-pack-v1.json"));
    Set<String> roles = strings(root.path("roles"));
    Set<String> actions = strings(root.path("scopes"));

    assertEquals(CapabilityPack.OPENWORKFLOW_V1.roles(), roles);
    assertEquals(
        java.util.Arrays.stream(AuthorizationAction.values())
            .map(AuthorizationAction::scope)
            .collect(Collectors.toUnmodifiableSet()),
        actions);
    assertEquals("26.7.1", root.path("keycloakVersion").asText());
    assertEquals("deny", root.at("/constraints/defaultDecision").asText());
    assertTrue(root.at("/constraints/activeOrganizationRequired").asBoolean());
    assertTrue(root.at("/constraints/topLevelRoleClaimsForbidden").asBoolean());
    assertFalse(root.at("/constraints/memberRoleAssignmentOnPackInstall").asBoolean());
  }

  @Test
  void entityIntelligencePolicyIsLeastPrivilegeAndMakerChecker() throws IOException {
    JsonNode root =
        new ObjectMapper()
            .readTree(
                CapabilityPackPolicyTest.class.getResourceAsStream(
                    "/entity-intelligence-capability-pack-v1.json"));

    assertEquals(CapabilityPack.ENTITY_INTELLIGENCE_V1.roles(), strings(root.path("roles")));
    assertEquals(
        CapabilityPack.ENTITY_INTELLIGENCE_V1.workloadIdentities(),
        strings(root.path("workloadIdentities")));
    assertFalse(
        strings(root.at("/roleGrants/entity-intelligence-reference-population-submitter"))
            .contains("reference-population:approve"));
    assertFalse(
        strings(root.at("/roleGrants/entity-intelligence-reference-population-approver"))
            .contains("reference-population:submit"));
    assertTrue(root.at("/constraints/referencePopulationMakerChecker").asBoolean());
    assertTrue(root.at("/constraints/databaseSchemaSelectorInTokensForbidden").asBoolean());
    assertTrue(root.at("/constraints/legacyWorkflowInternalForbidden").asBoolean());
  }

  private static Set<String> strings(JsonNode array) {
    return StreamSupport.stream(array.spliterator(), false)
        .map(JsonNode::asText)
        .collect(Collectors.toUnmodifiableSet());
  }
}
