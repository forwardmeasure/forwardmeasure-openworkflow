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

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.jpa.identity.entity.IdentityType;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowRequest;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkflowManagementResourceTest {
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          TenantId.parse("11111111-1111-1111-1111-111111111111"),
          "organization",
          "author",
          Set.of("definition-author"));

  @Test
  void implementsGeneratedCreateContractAndHttpMetadata() {
    var resource = new WorkflowManagementResource(new TestService(), () -> ACTOR, () -> "corr-1");

    try (var response =
        resource.createWorkflow(
            new CreateWorkflowRequest("order-processing", "Order processing")
                .description("Processes orders"))) {
      assertEquals(201, response.getStatus());
      assertEquals("\"0\"", response.getHeaderString("ETag"));
      assertEquals(
          "/v1/workflows/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
          response.getHeaderString("Location"));
    }
  }

  private static final class TestService implements WorkflowManagementService {
    @Override
    public Workflow createWorkflow(
        ActiveOrganization actor, String correlationId, CreateWorkflowRequest request) {
      assertEquals(ACTOR, actor);
      assertEquals("corr-1", correlationId);
      Workflow workflow =
          new Workflow(request.getName(), request.getTitle(), request.getDescription());
      workflow.setUuid(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
      workflow.setVersion(0);
      workflow.setOwner(
          Actor.builder()
              .id(1L)
              .uuid(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
              .subjectIdentifier(actor.actorId())
              .type(IdentityType.HUMAN)
              .identityProvider("test")
              .build());
      return workflow;
    }

    @Override
    public Page<Workflow> listWorkflows(
        ActiveOrganization actor, String correlationId, int offset, int limit) {
      return new Page<>(List.of(), 0, offset, limit);
    }

    @Override
    public Workflow getWorkflow(ActiveOrganization actor, String correlationId, UUID workflowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Workflow updateWorkflow(
        ActiveOrganization actor,
        String correlationId,
        UUID workflowId,
        int expectedVersion,
        UpdateWorkflowRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteWorkflow(
        ActiveOrganization actor, String correlationId, UUID workflowId, int expectedVersion) {
      throw new UnsupportedOperationException();
    }
  }
}
