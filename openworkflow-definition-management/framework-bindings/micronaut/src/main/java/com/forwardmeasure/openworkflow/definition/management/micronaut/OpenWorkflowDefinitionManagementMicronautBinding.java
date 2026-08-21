/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.definition.management.micronaut;

import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;

/** Micronaut composition owned by definition management, not the shared framework host. */
@Factory
public class OpenWorkflowDefinitionManagementMicronautBinding {

  @Singleton
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Singleton
  WorkflowManagementService workflowManagementService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization) {
    return new JpaWorkflowManagementService(
        entityManager, tenantScope, transactions, authorization);
  }

  @Singleton
  WorkflowGovernanceService workflowGovernanceService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    return new JpaWorkflowGovernanceService(
        entityManager, tenantScope, transactions, authorization, compiler);
  }
}
