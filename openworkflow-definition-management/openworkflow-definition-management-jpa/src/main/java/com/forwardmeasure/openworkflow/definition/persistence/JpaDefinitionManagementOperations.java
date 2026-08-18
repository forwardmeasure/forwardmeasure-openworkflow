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
package com.forwardmeasure.openworkflow.definition.persistence;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementOperations;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementService;
import com.forwardmeasure.openworkflow.definition.management.DefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.ManagedWorkflowRevision;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.util.List;
import java.util.Objects;

/** Transactional JPA composition of the portable governed-definition service. */
@Transactional
public class JpaDefinitionManagementOperations implements DefinitionManagementOperations {
  private final EntityManager entityManager;
  private final TenantScope tenantScope;
  private final AuthorizationService authorization;
  private final OpenWorkflowCompiler compiler;
  private final com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader resourceLoader;

  public JpaDefinitionManagementOperations(
      EntityManager entityManager,
      TenantScope tenantScope,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    this(
        entityManager,
        tenantScope,
        authorization,
        compiler,
        AllowlistedHttpWorkflowResourceLoader.fromEnvironment());
  }

  public JpaDefinitionManagementOperations(
      EntityManager entityManager,
      TenantScope tenantScope,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler,
      com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader resourceLoader) {
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.tenantScope = Objects.requireNonNull(tenantScope, "tenantScope");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.compiler = Objects.requireNonNull(compiler, "compiler");
    this.resourceLoader = Objects.requireNonNull(resourceLoader, "resourceLoader");
  }

  @Override
  public ManagedWorkflowRevision create(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument) {
    return inTenant(
        actor,
        () ->
            delegate(actor)
                .create(actor, correlationId, definitionKey, displayName, sourceDocument));
  }

  @Override
  public DefinitionValidation validate(
      ActiveOrganization actor, String correlationId, String definitionKey, String sourceDocument) {
    return inTenant(
        actor, () -> delegate(actor).validate(actor, correlationId, definitionKey, sourceDocument));
  }

  @Override
  public ManagedWorkflowRevision revise(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument) {
    return inTenant(
        actor,
        () ->
            delegate(actor)
                .revise(actor, correlationId, definitionKey, displayName, sourceDocument));
  }

  @Override
  public ManagedWorkflowRevision submit(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor, () -> delegate(actor).submit(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public ManagedWorkflowRevision withdraw(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor, () -> delegate(actor).withdraw(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public ManagedWorkflowRevision approve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor, () -> delegate(actor).approve(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public ManagedWorkflowRevision reject(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor, () -> delegate(actor).reject(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public ManagedWorkflowRevision publish(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor, () -> delegate(actor).publish(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public ManagedWorkflowRevision deprecate(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor,
        () -> delegate(actor).deprecate(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public ManagedWorkflowRevision retrieve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return inTenant(
        actor, () -> delegate(actor).retrieve(actor, correlationId, definitionKey, revisionNumber));
  }

  @Override
  public List<ManagedWorkflowRevision> list(ActiveOrganization actor, String correlationId) {
    return inTenant(actor, () -> delegate(actor).list(actor, correlationId));
  }

  private DefinitionManagementService delegate(ActiveOrganization actor) {
    return new DefinitionManagementService(
        new JpaDefinitionRepository(actor.tenantId(), entityManager),
        authorization,
        compiler,
        resourceLoader);
  }

  private <T> T inTenant(ActiveOrganization actor, java.util.function.Supplier<T> operation) {
    TenantSchema schema = TenantSchema.forTenant(actor.tenantId());
    return tenantScope.call(schema, operation);
  }
}
