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

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.management.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionStatus;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.mapper.DefinitionApiMapper;
import jakarta.ws.rs.core.Response;
import java.util.Objects;
import java.util.UUID;

/**
 * Implements {@link WorkflowDefinitionsApi} by delegating to {@link WorkflowGovernanceService}.
 * Covers the CRUD-shaped subset of that service (create/list/search/get/update/delete); the
 * remaining maker-checker transitions are {@link WorkflowDefinitionGovernanceResource}.
 */
public class WorkflowDefinitionResource implements WorkflowDefinitionsApi {
  private final WorkflowGovernanceService service;
  private final ActiveOrganizationProvider organizations;
  private final CorrelationIdProvider correlationIds;

  public WorkflowDefinitionResource(
      WorkflowGovernanceService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    this.service = Objects.requireNonNull(service, "service");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.correlationIds = Objects.requireNonNull(correlationIds, "correlationIds");
  }

  @Override
  public Response createWorkflowDefinition(
      UUID workflowId, CreateWorkflowDefinitionRequest createWorkflowDefinitionRequest) {
    WorkflowDefinition definition =
        service.createWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            createWorkflowDefinitionRequest);
    return Response.status(Response.Status.CREATED)
        .header("ETag", ETagSupport.tag(definition.getVersion()))
        .header("Location", "/v1/workflows/" + workflowId + "/definitions/" + definition.getUuid())
        .entity(DefinitionApiMapper.INSTANCE.toWorkflowDefinition(definition))
        .build();
  }

  @Override
  public Response deleteWorkflowDefinition(String ifMatch, UUID workflowId, UUID definitionId) {
    service.deleteWorkflowDefinition(
        organizations.current(),
        correlationIds.current(),
        workflowId,
        definitionId,
        ETagSupport.parseIfMatch(ifMatch));
    return Response.noContent().build();
  }

  @Override
  public Response getWorkflowDefinition(UUID workflowId, UUID definitionId) {
    WorkflowDefinition definition =
        service.getWorkflowDefinition(
            organizations.current(), correlationIds.current(), workflowId, definitionId);
    return ok(definition);
  }

  @Override
  public Response listWorkflowDefinitions(
      UUID workflowId, Integer offset, Integer limit, WorkflowDefinitionStatus status) {
    Page<WorkflowDefinition> page =
        service.listWorkflowDefinitions(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            toLifecycleState(status),
            offset,
            limit);
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflowDefinitionPage(page)).build();
  }

  @Override
  public Response searchWorkflowDefinitions(
      Integer offset, Integer limit, WorkflowDefinitionStatus status) {
    Page<WorkflowDefinition> page =
        service.searchWorkflowDefinitions(
            organizations.current(),
            correlationIds.current(),
            toLifecycleState(status),
            offset,
            limit);
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflowDefinitionPage(page)).build();
  }

  @Override
  public Response updateWorkflowDefinition(
      String ifMatch,
      UUID workflowId,
      UUID definitionId,
      UpdateWorkflowDefinitionRequest updateWorkflowDefinitionRequest) {
    WorkflowDefinition definition =
        service.updateWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch),
            updateWorkflowDefinitionRequest);
    return ok(definition);
  }

  private static Response ok(WorkflowDefinition definition) {
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflowDefinition(definition))
        .header("ETag", ETagSupport.tag(definition.getVersion()))
        .build();
  }

  private static WorkflowLifecycleState toLifecycleState(WorkflowDefinitionStatus status) {
    return status == null ? null : WorkflowLifecycleState.valueOf(status.name());
  }
}
