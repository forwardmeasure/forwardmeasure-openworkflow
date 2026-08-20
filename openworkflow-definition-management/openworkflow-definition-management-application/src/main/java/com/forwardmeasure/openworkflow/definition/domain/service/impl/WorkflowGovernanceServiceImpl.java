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
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowPublication;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowDefinitionRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowLifecycleHistoryRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowPublicationRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowReviewRepository;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.ReviewDecisionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class WorkflowGovernanceServiceImpl implements WorkflowGovernanceService {
  private final WorkflowRepository workflows;
  private final WorkflowDefinitionRepository definitions;
  private final WorkflowReviewRepository reviews;
  private final WorkflowPublicationRepository publications;
  private final WorkflowLifecycleHistoryRepository history;
  private final AuthorizationService authorization;
  private final OpenWorkflowCompiler compiler;
  private final WorkflowResourceLoader resourceLoader;

  public WorkflowGovernanceServiceImpl(
      WorkflowRepository workflows,
      WorkflowDefinitionRepository definitions,
      WorkflowReviewRepository reviews,
      WorkflowPublicationRepository publications,
      WorkflowLifecycleHistoryRepository history,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler,
      WorkflowResourceLoader resourceLoader) {
    this.workflows = Objects.requireNonNull(workflows, "workflows");
    this.definitions = Objects.requireNonNull(definitions, "definitions");
    this.reviews = Objects.requireNonNull(reviews, "reviews");
    this.publications = Objects.requireNonNull(publications, "publications");
    this.history = Objects.requireNonNull(history, "history");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.compiler = Objects.requireNonNull(compiler, "compiler");
    this.resourceLoader = Objects.requireNonNull(resourceLoader, "resourceLoader");
  }

  @Override
  public WorkflowDefinition createWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      CreateWorkflowDefinitionRequest request) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_CREATE);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowPlan plan = compile(request.getSource());
    if (!plan.coordinates().version().equals(request.getVersion())) {
      throw DefinitionManagementException.unprocessableEntity(
          "Requested version "
              + request.getVersion()
              + " does not match the version declared in the workflow source ("
              + plan.coordinates().version()
              + ")");
    }
    int revisionNumber = definitions.nextRevisionNumber(workflow);
    WorkflowDefinition definition =
        definitions.create(
            workflow,
            revisionNumber,
            actor.actorId(),
            request.getSource(),
            plan.definition().toString(),
            WorkflowResourceBundleCodec.encode(plan.resources()),
            plan.coordinates().namespace(),
            plan.coordinates().version(),
            plan.coordinates().dsl(),
            plan.compilerSha256(),
            plan.sourceSha256(),
            plan.definitionSha256());
    history.record(definition, null, WorkflowLifecycleState.DRAFT, actor.actorId(), correlationId);
    log.info("Created workflow definition {} for workflow {}", definition.getUuid(), workflowId);
    return definition;
  }

  @Override
  public Page<WorkflowDefinition> listWorkflowDefinitions(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      WorkflowLifecycleState status,
      int offset,
      int limit) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_LIST);
    Workflow workflow = requireWorkflow(workflowId);
    return definitions.listByWorkflow(workflow, status, offset, limit);
  }

  @Override
  public Page<WorkflowDefinition> searchWorkflowDefinitions(
      ActiveOrganization actor,
      String correlationId,
      WorkflowLifecycleState status,
      int offset,
      int limit) {
    authorize(actor, correlationId, "*", AuthorizationAction.DEFINITION_LIST);
    return definitions.search(status, offset, limit);
  }

  @Override
  public WorkflowDefinition getWorkflowDefinition(
      ActiveOrganization actor, String correlationId, UUID workflowId, UUID definitionId) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_READ);
    Workflow workflow = requireWorkflow(workflowId);
    return requireDefinition(workflow, definitionId);
  }

  @Override
  public WorkflowDefinition updateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      UpdateWorkflowDefinitionRequest request) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_UPDATE);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    requireVersion(definition, expectedVersion);
    requireDraft(definition, "updated");
    WorkflowPlan plan = compile(request.getSource());
    definition.setContent(
        request.getSource(),
        plan.definition().toString(),
        WorkflowResourceBundleCodec.encode(plan.resources()),
        plan.coordinates().namespace(),
        plan.coordinates().version(),
        plan.coordinates().dsl(),
        plan.compilerSha256(),
        plan.sourceSha256(),
        plan.definitionSha256());
    return definition;
  }

  @Override
  public void deleteWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_DELETE);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    requireVersion(definition, expectedVersion);
    requireDraft(definition, "deleted");
    definitions.delete(definition);
  }

  @Override
  public WorkflowDefinitionValidation validateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_VALIDATE);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    requireVersion(definition, expectedVersion);
    WorkflowPlan plan = compile(definition.getSourceDocument());
    return new WorkflowDefinitionValidation(
            definition.getUuid(),
            (long) definition.getVersion(),
            true,
            plan.sourceSha256(),
            java.util.List.<String>of())
        .definitionSha256(plan.definitionSha256())
        .compilerSha256(plan.compilerSha256());
  }

  @Override
  public WorkflowDefinition submitWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return transition(
        actor,
        correlationId,
        workflowId,
        definitionId,
        expectedVersion,
        AuthorizationAction.DEFINITION_SUBMIT,
        WorkflowLifecycleState.DRAFT,
        WorkflowLifecycleState.IN_REVIEW,
        false,
        null);
  }

  @Override
  public WorkflowDefinition withdrawWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return transition(
        actor,
        correlationId,
        workflowId,
        definitionId,
        expectedVersion,
        AuthorizationAction.DEFINITION_WITHDRAW,
        WorkflowLifecycleState.IN_REVIEW,
        WorkflowLifecycleState.DRAFT,
        false,
        null);
  }

  @Override
  public WorkflowDefinition approveWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      ReviewDecisionRequest request) {
    return transition(
        actor,
        correlationId,
        workflowId,
        definitionId,
        expectedVersion,
        AuthorizationAction.DEFINITION_APPROVE,
        WorkflowLifecycleState.IN_REVIEW,
        WorkflowLifecycleState.APPROVED,
        true,
        request == null ? null : request.getReason());
  }

  @Override
  public WorkflowDefinition rejectWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      ReviewDecisionRequest request) {
    return transition(
        actor,
        correlationId,
        workflowId,
        definitionId,
        expectedVersion,
        AuthorizationAction.DEFINITION_REJECT,
        WorkflowLifecycleState.IN_REVIEW,
        WorkflowLifecycleState.REJECTED,
        false,
        request == null ? null : request.getReason());
  }

  @Override
  public WorkflowDefinition publishWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_PUBLISH);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    if (definition.getLifecycleState() == WorkflowLifecycleState.PUBLISHED) {
      return definition;
    }
    requireVersion(definition, expectedVersion);
    requireState(definition, WorkflowLifecycleState.APPROVED, WorkflowLifecycleState.PUBLISHED);
    requireDifferentAuthor(definition, actor, AuthorizationAction.DEFINITION_PUBLISH);
    definition.transitionTo(WorkflowLifecycleState.PUBLISHED);
    history.record(
        definition,
        WorkflowLifecycleState.APPROVED,
        WorkflowLifecycleState.PUBLISHED,
        actor.actorId(),
        correlationId);
    publications.publish(definition, actor.actorId(), definition.getResolvedDigest());
    return definition;
  }

  @Override
  public WorkflowDefinition deprecateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    authorize(
        actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_DEPRECATE);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    requireVersion(definition, expectedVersion);
    requireState(definition, WorkflowLifecycleState.PUBLISHED, WorkflowLifecycleState.DEPRECATED);
    WorkflowPublication publication = definition.getPublication();
    if (publication == null) {
      throw DefinitionManagementException.conflict(
          "Cannot deprecate a definition with no publication record");
    }
    publication.deprecate();
    definition.transitionTo(WorkflowLifecycleState.DEPRECATED);
    history.record(
        definition,
        WorkflowLifecycleState.PUBLISHED,
        WorkflowLifecycleState.DEPRECATED,
        actor.actorId(),
        correlationId);
    return definition;
  }

  private WorkflowDefinition transition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      AuthorizationAction action,
      WorkflowLifecycleState expected,
      WorkflowLifecycleState target,
      boolean makerChecker,
      String reason) {
    authorize(actor, correlationId, workflowId.toString(), action);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    requireVersion(definition, expectedVersion);
    requireState(definition, expected, target);
    if (makerChecker) {
      requireDifferentAuthor(definition, actor, action);
    }
    WorkflowLifecycleState from = definition.getLifecycleState();
    definition.transitionTo(target);
    history.record(definition, from, target, actor.actorId(), correlationId);
    if (target == WorkflowLifecycleState.APPROVED || target == WorkflowLifecycleState.REJECTED) {
      reviews.record(
          definition, target.name(), actor.actorId(), definition.getResolvedDigest(), reason);
    }
    return definition;
  }

  private void requireDifferentAuthor(
      WorkflowDefinition definition, ActiveOrganization actor, AuthorizationAction action) {
    if (definition.getAuthor().getSubjectIdentifier().equals(actor.actorId())) {
      throw DefinitionManagementException.conflict(
          "The definition author cannot " + action.scope() + " their own revision");
    }
  }

  private static void requireState(
      WorkflowDefinition definition,
      WorkflowLifecycleState expected,
      WorkflowLifecycleState target) {
    if (definition.getLifecycleState() != expected) {
      throw DefinitionManagementException.conflict(
          "Cannot transition " + definition.getLifecycleState() + " to " + target);
    }
  }

  private static void requireDraft(WorkflowDefinition definition, String action) {
    if (definition.getLifecycleState() != WorkflowLifecycleState.DRAFT) {
      throw DefinitionManagementException.conflict(
          "A definition can only be " + action + " while it is draft");
    }
  }

  private static void requireVersion(AbstractBaseEntity<?> entity, int expectedVersion) {
    Integer actual = entity.getVersion();
    if (actual == null || actual != expectedVersion) {
      throw DefinitionManagementException.preconditionFailed(
          "If-Match does not identify the current revision (expected " + actual + ")");
    }
  }

  private Workflow requireWorkflow(UUID workflowId) {
    return workflows
        .findById(workflowId)
        .orElseThrow(
            () -> DefinitionManagementException.notFound("Workflow not found: " + workflowId));
  }

  private WorkflowDefinition requireDefinition(Workflow workflow, UUID definitionId) {
    return definitions
        .findByWorkflowAndUuid(workflow, definitionId)
        .orElseThrow(
            () ->
                DefinitionManagementException.notFound(
                    "Workflow definition not found: " + definitionId));
  }

  private WorkflowPlan compile(String sourceDocument) {
    Objects.requireNonNull(sourceDocument, "sourceDocument");
    byte[] source = sourceDocument.getBytes(StandardCharsets.UTF_8);
    return compiler.compile(source, new WorkflowResourceResolver().resolve(source, resourceLoader));
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
