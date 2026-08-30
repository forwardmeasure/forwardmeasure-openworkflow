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
import com.forwardmeasure.openworkflow.definition.WorkflowDefinitionException;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
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
    int revisionNumber = definitions.nextRevisionNumber(workflow);
    WorkflowDefinition definition;
    try {
      WorkflowPlan plan = compile(request.getSource());
      if (!plan.coordinates().version().equals(request.getVersion())) {
        throw DefinitionManagementException.unprocessableEntity(
            "Requested version "
                + request.getVersion()
                + " does not match the version declared in the workflow source ("
                + plan.coordinates().version()
                + ")");
      }
      definition =
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
    } catch (WorkflowDefinitionException notYetCompilable) {
      // A DRAFT doesn't have to compile to be saved - only submitWorkflowDefinition enforces
      // that. request.getVersion() (not anything derived from the source, since nothing compiled
      // it) is what's trusted for documentVersion here, so "find the draft for version X" lookups
      // still work before there's anything to compile. Re-thrown, not swallowed, on failure.
      definition =
          definitions.create(
              workflow,
              revisionNumber,
              actor.actorId(),
              request.getSource(),
              sha256(request.getSource()),
              request.getVersion());
    }
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
    try {
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
    } catch (WorkflowDefinitionException notYetCompilable) {
      // Same "a DRAFT doesn't have to compile" carve-out as createWorkflowDefinition - only the
      // raw source changes; every compiled-derived field is left exactly as it was (see
      // setSourceOnly's own doc comment for why that's safe to leave stale rather than null it
      // out). Re-thrown, not swallowed, on failure.
      definition.setSourceOnly(request.getSource(), sha256(request.getSource()));
    }
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
    // Not a plain transition() (unlike withdraw/approve/reject below) - submit is the actual
    // enforcement point now that draft saves no longer have to compile. Recompiles here, for real,
    // populating every compiled-derived field for the first time if createWorkflowDefinition/
    // updateWorkflowDefinition never managed to; a definition that still doesn't compile stays
    // DRAFT (the exception propagates uncaught - now a clean 422 via
    // WorkflowDefinitionExceptionMapper, not a state change).
    //
    // Transitions straight to APPROVED, not IN_REVIEW - "cut the manual approve/reject gate,
    // makes no sense" (explicit product decision). Submit and publish stay two separate steps
    // (submit still means "compiled and frozen," publish still means "actually live"), but there's
    // no longer a manual human-review gate between them: a successful compile IS the approval.
    // IN_REVIEW/approveWorkflowDefinition/rejectWorkflowDefinition are left in place (unreachable
    // by this path, but not deleted) only so a definition already sitting in IN_REVIEW from before
    // this change - requireState below still accepts it as a submit source - has a way through.
    authorize(actor, correlationId, workflowId.toString(), AuthorizationAction.DEFINITION_SUBMIT);
    Workflow workflow = requireWorkflow(workflowId);
    WorkflowDefinition definition = requireDefinition(workflow, definitionId);
    requireVersion(definition, expectedVersion);
    requireState(definition, WorkflowLifecycleState.DRAFT, WorkflowLifecycleState.IN_REVIEW);
    WorkflowPlan plan = compile(definition.getSourceDocument());
    definition.setContent(
        definition.getSourceDocument(),
        plan.definition().toString(),
        WorkflowResourceBundleCodec.encode(plan.resources()),
        plan.coordinates().namespace(),
        plan.coordinates().version(),
        plan.coordinates().dsl(),
        plan.compilerSha256(),
        plan.sourceSha256(),
        plan.definitionSha256());
    definition.transitionTo(WorkflowLifecycleState.APPROVED);
    history.record(
        definition,
        WorkflowLifecycleState.DRAFT,
        WorkflowLifecycleState.APPROVED,
        actor.actorId(),
        correlationId);
    return definition;
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
    WorkflowPublication publication =
        publications.publish(definition, actor.actorId(), definition.getResolvedDigest());
    definition.attachPublication(publication);
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

  // Mirrors OpenWorkflowCompiler's own private sha256() exactly (same HexFormat lowercase-hex
  // encoding the WorkflowDefinition entity's digest() validator requires) - needed independently
  // here because a failed compile() never returns a WorkflowPlan to read sourceSha256() off of.
  private static String sha256(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
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
