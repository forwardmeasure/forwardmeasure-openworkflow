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

import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.management.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.api.model.ReviewDecisionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.mapper.DefinitionApiMapper;
import jakarta.ws.rs.core.Response;
import java.util.Objects;
import java.util.UUID;

/**
 * Implements {@link WorkflowDefinitionGovernanceApi}: validate and the six maker-checker
 * transitions (submit, withdraw, approve, reject, publish, deprecate), all delegating to {@link
 * WorkflowGovernanceService}.
 */
public class WorkflowDefinitionGovernanceResource implements WorkflowDefinitionGovernanceApi {
  private final WorkflowGovernanceService service;
  private final ActiveOrganizationProvider organizations;
  private final CorrelationIdProvider correlationIds;

  public WorkflowDefinitionGovernanceResource(
      WorkflowGovernanceService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    this.service = Objects.requireNonNull(service, "service");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.correlationIds = Objects.requireNonNull(correlationIds, "correlationIds");
  }

  @Override
  public Response approveWorkflowDefinition(
      String ifMatch,
      UUID workflowId,
      UUID definitionId,
      ReviewDecisionRequest reviewDecisionRequest) {
    return ok(
        service.approveWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch),
            reviewDecisionRequest));
  }

  @Override
  public Response deprecateWorkflowDefinition(String ifMatch, UUID workflowId, UUID definitionId) {
    return ok(
        service.deprecateWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch)));
  }

  @Override
  public Response publishWorkflowDefinition(String ifMatch, UUID workflowId, UUID definitionId) {
    return ok(
        service.publishWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch)));
  }

  @Override
  public Response rejectWorkflowDefinition(
      String ifMatch,
      UUID workflowId,
      UUID definitionId,
      ReviewDecisionRequest reviewDecisionRequest) {
    return ok(
        service.rejectWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch),
            reviewDecisionRequest));
  }

  @Override
  public Response submitWorkflowDefinition(String ifMatch, UUID workflowId, UUID definitionId) {
    return ok(
        service.submitWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch)));
  }

  @Override
  public Response validateWorkflowDefinition(String ifMatch, UUID workflowId, UUID definitionId) {
    WorkflowDefinitionValidation validation =
        service.validateWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch));
    return Response.ok(validation).build();
  }

  @Override
  public Response withdrawWorkflowDefinition(String ifMatch, UUID workflowId, UUID definitionId) {
    return ok(
        service.withdrawWorkflowDefinition(
            organizations.current(),
            correlationIds.current(),
            workflowId,
            definitionId,
            ETagSupport.parseIfMatch(ifMatch)));
  }

  private static Response ok(WorkflowDefinition definition) {
    return Response.ok(DefinitionApiMapper.INSTANCE.toWorkflowDefinition(definition))
        .header("ETag", ETagSupport.tag(definition.getVersion()))
        .build();
  }
}
