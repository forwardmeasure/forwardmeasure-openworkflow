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
package com.forwardmeasure.openworkflow.definition.domain.service.impl;

import com.forwardmeasure.jpa.core.entity.AbstractBaseEntity;
import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowRepository;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowRequest;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class WorkflowManagementServiceImpl implements WorkflowManagementService {
  private final WorkflowRepository repository;
  private final AuthorizationService authorization;

  public WorkflowManagementServiceImpl(
      WorkflowRepository repository, AuthorizationService authorization) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
  }

  @Override
  public Workflow createWorkflow(
      ActiveOrganization actor, String correlationId, CreateWorkflowRequest request) {
    authorize(actor, correlationId, request.getName(), AuthorizationAction.DEFINITION_CREATE);
    if (repository.existsByName(request.getName())) {
      throw DefinitionManagementException.conflict("Workflow already exists: " + request.getName());
    }
    Workflow workflow =
        repository.create(
            actor.actorId(), request.getName(), request.getTitle(), request.getDescription());
    log.info("Created workflow {} ({})", workflow.getUuid(), workflow.getName());
    return workflow;
  }

  @Override
  public Page<Workflow> listWorkflows(
      ActiveOrganization actor, String correlationId, int offset, int limit) {
    authorize(actor, correlationId, "*", AuthorizationAction.DEFINITION_LIST);
    return repository.list(offset, limit);
  }

  @Override
  public Workflow getWorkflow(ActiveOrganization actor, String correlationId, UUID workflowId) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_READ);
    return requireWorkflow(workflowId);
  }

  @Override
  public Workflow updateWorkflow(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      int expectedVersion,
      UpdateWorkflowRequest request) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_UPDATE);
    Workflow workflow = requireWorkflow(workflowId);
    requireVersion(workflow, expectedVersion);
    workflow.setName(request.getName());
    workflow.setTitle(request.getTitle());
    workflow.setDescription(request.getDescription());
    return workflow;
  }

  @Override
  public void deleteWorkflow(
      ActiveOrganization actor, String correlationId, UUID workflowId, int expectedVersion) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_DELETE);
    Workflow workflow = requireWorkflow(workflowId);
    requireVersion(workflow, expectedVersion);
    repository.delete(workflow);
    log.info("Deleted workflow {}", workflowId);
  }

  private Workflow requireWorkflow(UUID workflowId) {
    return repository
        .findById(workflowId)
        .orElseThrow(
            () -> DefinitionManagementException.notFound("Workflow not found: " + workflowId));
  }

  private static void requireVersion(AbstractBaseEntity<?> entity, int expectedVersion) {
    Integer actual = entity.getVersion();
    if (actual == null || actual != expectedVersion) {
      throw DefinitionManagementException.preconditionFailed(
          "If-Match does not identify the current revision (expected " + actual + ")");
    }
  }

  private void authorize(
      ActiveOrganization actor,
      String correlationId,
      String resourceId,
      AuthorizationAction action) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor, AuthorizationResource.definition(resourceId), action, correlationId, Map.of()));
  }
}
