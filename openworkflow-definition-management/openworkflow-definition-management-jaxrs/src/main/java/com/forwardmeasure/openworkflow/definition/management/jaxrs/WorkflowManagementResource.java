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
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.management.api.WorkflowsApi;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.mapper.DefinitionApiMapper;
import jakarta.ws.rs.core.Response;
import java.util.Objects;
import java.util.UUID;

/** Implements {@link WorkflowsApi} by delegating to {@link WorkflowManagementService}. */
public class WorkflowManagementResource implements WorkflowsApi {
  private final WorkflowManagementService service;
  private final ActiveOrganizationProvider organizations;
  private final CorrelationIdProvider correlationIds;

  public WorkflowManagementResource(
      WorkflowManagementService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    this.service = Objects.requireNonNull(service, "service");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.correlationIds = Objects.requireNonNull(correlationIds, "correlationIds");
  }

  @Override
  public Response createWorkflow(CreateWorkflowRequest createWorkflowRequest) {
    Workflow workflow =
        service.createWorkflow(
            organizations.current(), correlationIds.current(), createWorkflowRequest);
    return Response.status(Response.Status.CREATED)
        .header("ETag", ETagSupport.tag(workflow.getVersion()))
        .header("Location", "/v1/workflows/" + workflow.getUuid())
        .entity(DefinitionApiMapper.INSTANCE.toWorkflow(workflow))
        .build();
  }

  @Override
  public Response deleteWorkflow(String ifMatch, UUID workflowId) {
    service.deleteWorkflow(
        organizations.current(),
        correlationIds.current(),
        workflowId,
        ETagSupport.parseIfMatch(ifMatch));
    return Response.noContent().build();
  }

  @Override
  public Response getWorkflow(UUID workflowId) {
    Workflow workflow =
        service.getWorkflow(organizations.current(), correlationIds.current(), workflowId);
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflow(workflow))
        .header("ETag", ETagSupport.tag(workflow.getVersion()))
        .build();
  }

  @Override
  public Response listWorkflows(Integer offset, Integer limit) {
    Page<Workflow> page =
        service.listWorkflows(organizations.current(), correlationIds.current(), offset, limit);
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflowPage(page)).build();
  }

  @Override
  public Response updateWorkflow(
      String ifMatch, UUID workflowId, UpdateWorkflowRequest updateWorkflowRequest) {
    Workflow workflow =
        service.updateWorkflow(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            ETagSupport.parseIfMatch(ifMatch),
            updateWorkflowRequest);
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflow(workflow))
        .header("ETag", ETagSupport.tag(workflow.getVersion()))
        .build();
  }
}
