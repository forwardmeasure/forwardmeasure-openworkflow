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
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.domain.service.impl.WorkflowManagementServiceImpl;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowRepository;
import jakarta.persistence.EntityManager;
import java.util.Objects;
import java.util.UUID;

/**
 * Tenant-scoped, transactional composition of {@link WorkflowManagementService}. Every method opens
 * tenant scope, then runs a transaction inside it, then constructs fresh repositories bound to the
 * {@link EntityManager} and a fresh {@link WorkflowManagementServiceImpl} to do the work — all as
 * plain nested method calls, no declarative {@code @Transactional} interceptor involved.
 */
public class JpaWorkflowManagementService implements WorkflowManagementService {
  private final EntityManager entityManager;
  private final TenantScope tenantScope;
  private final WorkflowTransactionExecutor transactions;
  private final AuthorizationService authorization;

  public JpaWorkflowManagementService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization) {
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.tenantScope = Objects.requireNonNull(tenantScope, "tenantScope");
    this.transactions = Objects.requireNonNull(transactions, "transactions");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
  }

  @Override
  public Workflow createWorkflow(
      ActiveOrganization actor, String correlationId, CreateWorkflowRequest request) {
    return inTenant(actor, () -> delegate().createWorkflow(actor, correlationId, request));
  }

  @Override
  public Page<Workflow> listWorkflows(
      ActiveOrganization actor, String correlationId, int offset, int limit) {
    return inTenant(actor, () -> delegate().listWorkflows(actor, correlationId, offset, limit));
  }

  @Override
  public Workflow getWorkflow(ActiveOrganization actor, String correlationId, UUID workflowId) {
    return inTenant(actor, () -> delegate().getWorkflow(actor, correlationId, workflowId));
  }

  @Override
  public Workflow updateWorkflow(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      int expectedVersion,
      UpdateWorkflowRequest request) {
    return inTenant(
        actor,
        () ->
            delegate().updateWorkflow(actor, correlationId, workflowId, expectedVersion, request));
  }

  @Override
  public void deleteWorkflow(
      ActiveOrganization actor, String correlationId, UUID workflowId, int expectedVersion) {
    inTenant(
        actor,
        () -> {
          delegate().deleteWorkflow(actor, correlationId, workflowId, expectedVersion);
          return null;
        });
  }

  private WorkflowManagementService delegate() {
    return new WorkflowManagementServiceImpl(
        new JpaWorkflowRepository(entityManager), authorization);
  }

  private <T> T inTenant(ActiveOrganization actor, java.util.function.Supplier<T> operation) {
    TenantSchema schema = TenantSchema.forTenant(actor.tenantId());
    return tenantScope.call(schema, () -> transactions.execute(operation));
  }
}
