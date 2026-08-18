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
package com.forwardmeasure.openworkflow.definition.management;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Framework-neutral governed definition use cases. Transaction boundaries belong to adapters. */
public final class DefinitionManagementService implements DefinitionManagementOperations {
  private final DefinitionRepository repository;
  private final AuthorizationService authorization;
  private final OpenWorkflowCompiler compiler;
  private final WorkflowResourceLoader resourceLoader;

  public DefinitionManagementService(
      DefinitionRepository repository,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    this(
        repository,
        authorization,
        compiler,
        request -> {
          throw new IllegalArgumentException(
              "No publication resource loader is configured for " + request.uri());
        });
  }

  public DefinitionManagementService(
      DefinitionRepository repository,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler,
      WorkflowResourceLoader resourceLoader) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.compiler = Objects.requireNonNull(compiler, "compiler");
    this.resourceLoader = Objects.requireNonNull(resourceLoader, "resourceLoader");
  }

  public ManagedWorkflowRevision create(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument) {
    authorize(actor, correlationId, definitionKey, AuthorizationAction.DEFINITION_CREATE);
    if (repository.exists(actor.tenantId(), definitionKey)) {
      throw new DefinitionManagementException("Definition already exists: " + definitionKey);
    }
    ManagedWorkflowRevision revision =
        compile(actor, definitionKey, displayName, 1, sourceDocument);
    repository.save(actor.tenantId(), revision, actor.actorId(), correlationId);
    return revision;
  }

  public DefinitionValidation validate(
      ActiveOrganization actor, String correlationId, String definitionKey, String sourceDocument) {
    authorize(actor, correlationId, definitionKey, AuthorizationAction.DEFINITION_VALIDATE);
    WorkflowPlan plan = compile(sourceDocument);
    return validation(plan);
  }

  public ManagedWorkflowRevision revise(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument) {
    authorize(actor, correlationId, definitionKey, AuthorizationAction.DEFINITION_UPDATE);
    if (!repository.exists(actor.tenantId(), definitionKey)) {
      throw notFound(definitionKey, 0);
    }
    int number = repository.nextRevisionNumber(actor.tenantId(), definitionKey);
    ManagedWorkflowRevision revision =
        compile(actor, definitionKey, displayName, number, sourceDocument);
    repository.save(actor.tenantId(), revision, actor.actorId(), correlationId);
    return revision;
  }

  public ManagedWorkflowRevision submit(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return transition(
        actor,
        correlationId,
        definitionKey,
        revisionNumber,
        AuthorizationAction.DEFINITION_SUBMIT,
        WorkflowLifecycleState.DRAFT,
        WorkflowLifecycleState.IN_REVIEW,
        false);
  }

  public ManagedWorkflowRevision withdraw(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return transition(
        actor,
        correlationId,
        definitionKey,
        revisionNumber,
        AuthorizationAction.DEFINITION_WITHDRAW,
        WorkflowLifecycleState.IN_REVIEW,
        WorkflowLifecycleState.DRAFT,
        false);
  }

  public ManagedWorkflowRevision approve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return transition(
        actor,
        correlationId,
        definitionKey,
        revisionNumber,
        AuthorizationAction.DEFINITION_APPROVE,
        WorkflowLifecycleState.IN_REVIEW,
        WorkflowLifecycleState.APPROVED,
        true);
  }

  public ManagedWorkflowRevision reject(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return transition(
        actor,
        correlationId,
        definitionKey,
        revisionNumber,
        AuthorizationAction.DEFINITION_REJECT,
        WorkflowLifecycleState.IN_REVIEW,
        WorkflowLifecycleState.REJECTED,
        false);
  }

  public ManagedWorkflowRevision publish(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return transition(
        actor,
        correlationId,
        definitionKey,
        revisionNumber,
        AuthorizationAction.DEFINITION_PUBLISH,
        WorkflowLifecycleState.APPROVED,
        WorkflowLifecycleState.PUBLISHED,
        true);
  }

  public ManagedWorkflowRevision deprecate(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return transition(
        actor,
        correlationId,
        definitionKey,
        revisionNumber,
        AuthorizationAction.DEFINITION_DEPRECATE,
        WorkflowLifecycleState.PUBLISHED,
        WorkflowLifecycleState.DEPRECATED,
        false);
  }

  public ManagedWorkflowRevision retrieve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    authorize(actor, correlationId, definitionKey, AuthorizationAction.DEFINITION_READ);
    return requireRevision(actor, definitionKey, revisionNumber);
  }

  public List<ManagedWorkflowRevision> list(ActiveOrganization actor, String correlationId) {
    authorize(actor, correlationId, "*", AuthorizationAction.DEFINITION_LIST);
    return List.copyOf(repository.list(actor.tenantId()));
  }

  private ManagedWorkflowRevision transition(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      int revisionNumber,
      AuthorizationAction action,
      WorkflowLifecycleState expected,
      WorkflowLifecycleState target,
      boolean makerChecker) {
    authorize(actor, correlationId, definitionKey, action);
    ManagedWorkflowRevision current = requireRevision(actor, definitionKey, revisionNumber);
    if (current.lifecycleState() != expected) {
      throw new DefinitionManagementException(
          "Cannot transition " + current.lifecycleState() + " to " + target);
    }
    if (makerChecker && current.authorActorId().equals(actor.actorId())) {
      throw new DefinitionManagementException(
          "The revision author cannot " + action.scope() + " their own revision");
    }
    ManagedWorkflowRevision changed = current.transitionTo(target);
    repository.save(actor.tenantId(), changed, actor.actorId(), correlationId);
    return changed;
  }

  private ManagedWorkflowRevision requireRevision(
      ActiveOrganization actor, String definitionKey, int revisionNumber) {
    return repository
        .find(actor.tenantId(), definitionKey, revisionNumber)
        .orElseThrow(() -> notFound(definitionKey, revisionNumber));
  }

  private ManagedWorkflowRevision compile(
      ActiveOrganization actor,
      String definitionKey,
      String displayName,
      int revisionNumber,
      String sourceDocument) {
    WorkflowPlan plan = compile(sourceDocument);
    return new ManagedWorkflowRevision(
        UUID.randomUUID(),
        definitionKey,
        displayName,
        revisionNumber,
        WorkflowLifecycleState.DRAFT,
        sourceDocument,
        plan.definition().toString(),
        plan.resources(),
        plan.coordinates().dsl(),
        plan.compilerSha256(),
        plan.sourceSha256(),
        plan.definitionSha256(),
        actor.actorId());
  }

  private WorkflowPlan compile(String sourceDocument) {
    Objects.requireNonNull(sourceDocument, "sourceDocument");
    byte[] source = sourceDocument.getBytes(StandardCharsets.UTF_8);
    return compiler.compile(source, new WorkflowResourceResolver().resolve(source, resourceLoader));
  }

  private static DefinitionValidation validation(WorkflowPlan plan) {
    return new DefinitionValidation(
        plan.sourceSha256(),
        plan.definitionSha256(),
        plan.coordinates().dsl(),
        plan.compilerSha256());
  }

  private void authorize(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      AuthorizationAction action) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            AuthorizationResource.definition(definitionKey),
            action,
            correlationId,
            Map.of()));
  }

  private static DefinitionManagementException notFound(String key, int revisionNumber) {
    String revision = revisionNumber < 1 ? "" : " revision " + revisionNumber;
    return new DefinitionManagementException("Definition not found: " + key + revision);
  }
}
