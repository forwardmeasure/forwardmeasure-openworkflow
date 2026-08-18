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
package com.forwardmeasure.openworkflow.definition.management.jaxrs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementService;
import com.forwardmeasure.openworkflow.definition.management.DefinitionRepository;
import com.forwardmeasure.openworkflow.definition.management.ManagedWorkflowRevision;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionRevision;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionWrite;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DefinitionManagementResourceTest {
  private static final TenantId TENANT = TenantId.parse("11111111-1111-1111-1111-111111111111");

  @Test
  void implementsGeneratedCreateContractUsingTrustedOrganization() {
    var repository = new MemoryRepository();
    AuthorizationService authorization =
        new AuthorizationService() {
          @Override
          public AuthorizationDecision evaluate(AuthorizationRequest request) {
            return new AuthorizationDecision(true, request.correlationId(), Map.of());
          }

          @Override
          public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
            return requests.stream().map(this::evaluate).toList();
          }
        };
    ActiveOrganization actor =
        new ActiveOrganization(TENANT, "organization", "author", Set.of("definition-author"));
    var resource =
        new DefinitionManagementResource(
            new DefinitionManagementService(repository, authorization, new OpenWorkflowCompiler()),
            () -> actor);
    var request =
        new DefinitionWrite(
            "orders",
            "Orders",
            """
            document:
              dsl: '1.0.3'
              namespace: tests
              name: orders
              version: '1.0.0'
            do:
              - initialize:
                  set:
                    ready: true
            """);

    try (var response = resource.createDefinition("correlation", request)) {
      assertEquals(201, response.getStatus());
      DefinitionRevision body = (DefinitionRevision) response.getEntity();
      assertEquals("orders", body.getDefinitionKey());
      assertEquals("author", body.getAuthorActorId());
      assertEquals("DRAFT", body.getLifecycleState().toString());
    }
  }

  private static final class MemoryRepository implements DefinitionRepository {
    private final List<ManagedWorkflowRevision> values = new ArrayList<>();

    @Override
    public boolean exists(TenantId tenantId, String key) {
      return values.stream().anyMatch(value -> value.definitionKey().equals(key));
    }

    @Override
    public int nextRevisionNumber(TenantId tenantId, String key) {
      return values.size() + 1;
    }

    @Override
    public void save(
        TenantId tenantId,
        ManagedWorkflowRevision revision,
        String actingActorId,
        String correlationId) {
      values.add(revision);
    }

    @Override
    public Optional<ManagedWorkflowRevision> find(TenantId tenantId, String key, int number) {
      return values.stream()
          .filter(value -> value.definitionKey().equals(key) && value.revisionNumber() == number)
          .findFirst();
    }

    @Override
    public List<ManagedWorkflowRevision> list(TenantId tenantId) {
      return List.copyOf(values);
    }
  }
}
