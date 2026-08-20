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
package com.forwardmeasure.openworkflow.definition.domain.service.jpa;

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.impl.WorkflowGovernanceServiceImpl;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.AllowlistedHttpWorkflowResourceLoader;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.ReviewDecisionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowDefinitionRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowLifecycleHistoryRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowPublicationRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowReviewRepository;
import jakarta.persistence.EntityManager;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Tenant-scoped, transactional composition of {@link WorkflowGovernanceService} — see {@link
 * JpaWorkflowManagementService} for the shared tenant/transaction pattern this mirrors.
 */
public class JpaWorkflowGovernanceService implements WorkflowGovernanceService {
  private final EntityManager entityManager;
  private final TenantScope tenantScope;
  private final WorkflowTransactionExecutor transactions;
  private final AuthorizationService authorization;
  private final OpenWorkflowCompiler compiler;
  private final WorkflowResourceLoader resourceLoader;

  public JpaWorkflowGovernanceService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    this(
        entityManager,
        tenantScope,
        transactions,
        authorization,
        compiler,
        AllowlistedHttpWorkflowResourceLoader.fromEnvironment());
  }

  public JpaWorkflowGovernanceService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler,
      WorkflowResourceLoader resourceLoader) {
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.tenantScope = Objects.requireNonNull(tenantScope, "tenantScope");
    this.transactions = Objects.requireNonNull(transactions, "transactions");
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
    return inTenant(
        actor,
        () -> delegate().createWorkflowDefinition(actor, correlationId, workflowId, request));
  }

  @Override
  public Page<WorkflowDefinition> listWorkflowDefinitions(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      WorkflowLifecycleState status,
      int offset,
      int limit) {
    return inTenant(
        actor,
        () ->
            delegate()
                .listWorkflowDefinitions(actor, correlationId, workflowId, status, offset, limit));
  }

  @Override
  public Page<WorkflowDefinition> searchWorkflowDefinitions(
      ActiveOrganization actor,
      String correlationId,
      WorkflowLifecycleState status,
      int offset,
      int limit) {
    return inTenant(
        actor,
        () -> delegate().searchWorkflowDefinitions(actor, correlationId, status, offset, limit));
  }

  @Override
  public WorkflowDefinition getWorkflowDefinition(
      ActiveOrganization actor, String correlationId, UUID workflowId, UUID definitionId) {
    return inTenant(
        actor,
        () -> delegate().getWorkflowDefinition(actor, correlationId, workflowId, definitionId));
  }

  @Override
  public WorkflowDefinition updateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      UpdateWorkflowDefinitionRequest request) {
    return inTenant(
        actor,
        () ->
            delegate()
                .updateWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion, request));
  }

  @Override
  public void deleteWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    inTenant(
        actor,
        () -> {
          delegate()
              .deleteWorkflowDefinition(
                  actor, correlationId, workflowId, definitionId, expectedVersion);
          return null;
        });
  }

  @Override
  public WorkflowDefinitionValidation validateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return inTenant(
        actor,
        () ->
            delegate()
                .validateWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion));
  }

  @Override
  public WorkflowDefinition submitWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return inTenant(
        actor,
        () ->
            delegate()
                .submitWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion));
  }

  @Override
  public WorkflowDefinition withdrawWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return inTenant(
        actor,
        () ->
            delegate()
                .withdrawWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion));
  }

  @Override
  public WorkflowDefinition approveWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      ReviewDecisionRequest request) {
    return inTenant(
        actor,
        () ->
            delegate()
                .approveWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion, request));
  }

  @Override
  public WorkflowDefinition rejectWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      ReviewDecisionRequest request) {
    return inTenant(
        actor,
        () ->
            delegate()
                .rejectWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion, request));
  }

  @Override
  public WorkflowDefinition publishWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return inTenant(
        actor,
        () ->
            delegate()
                .publishWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion));
  }

  @Override
  public WorkflowDefinition deprecateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion) {
    return inTenant(
        actor,
        () ->
            delegate()
                .deprecateWorkflowDefinition(
                    actor, correlationId, workflowId, definitionId, expectedVersion));
  }

  private WorkflowGovernanceService delegate() {
    return new WorkflowGovernanceServiceImpl(
        new JpaWorkflowRepository(entityManager),
        new JpaWorkflowDefinitionRepository(entityManager),
        new JpaWorkflowReviewRepository(entityManager),
        new JpaWorkflowPublicationRepository(entityManager),
        new JpaWorkflowLifecycleHistoryRepository(entityManager),
        authorization,
        compiler,
        resourceLoader);
  }

  private <T> T inTenant(ActiveOrganization actor, Supplier<T> operation) {
    TenantSchema schema = TenantSchema.forTenant(actor.tenantId());
    return tenantScope.call(schema, () -> transactions.execute(operation));
  }
}
